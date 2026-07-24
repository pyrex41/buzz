/**
 * React Query wiring for the Workstream feature.
 *
 * Reads are one-shot fetches keyed by channel or workstream address; a live
 * subscription invalidates them so a teammate's edit lands on the board
 * without a refresh. Invalidate-rather-than-merge is deliberate: the refetch
 * re-runs the same LWW reduce over the relay's authoritative set, so a live
 * event that arrives out of order cannot leave the board in a state the
 * relay would disagree with.
 */

import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { relayClient } from "@/shared/api/relayClient";
import type { RelayEvent } from "@/shared/api/types";
import {
  KIND_ARTIFACT,
  KIND_ARTIFACT_VERSION,
  KIND_DECISION_RECORD,
  KIND_REVIEW_COMMENT,
  KIND_REVIEW_DECISION,
  KIND_REVIEW_REQUEST,
  KIND_TASK_STATUS_CHANGE,
  KIND_WORKSTREAM,
  KIND_WORKSTREAM_TASK,
} from "@/shared/constants/kinds";

import { resolveHeadList } from "./lib/heads";
import {
  type Artifact,
  type DecisionRecord,
  type WorkstreamTask,
  parseArtifact,
  parseDecision,
  parseTask,
  parseWorkstream,
} from "./lib/parse";
import {
  changeTaskStatus,
  commentOnReview,
  decideReview,
  fetchReviewComments,
  fetchWorkstreamById,
  fetchWorkstreamChildren,
  fetchWorkstreamHistory,
  fetchWorkstreams,
  requestReview,
  saveArtifact,
  saveDecision,
  saveTask,
  saveWorkstream,
} from "./lib/workstreamService";

export const workstreamsQueryKey = (channelId: string) =>
  ["workstreams", channelId] as const;
export const workstreamQueryKey = (workstreamId: string) =>
  ["workstream-head", workstreamId] as const;
export const workstreamChildrenQueryKey = (address: string) =>
  ["workstream-children", address] as const;
export const workstreamHistoryQueryKey = (address: string) =>
  ["workstream-history", address] as const;
export const reviewCommentsQueryKey = (address: string) =>
  ["workstream-review-comments", address] as const;

/** Every kind whose arrival should refresh a workstream view. */
const WORKSTREAM_LIVE_KINDS = [
  KIND_WORKSTREAM,
  KIND_WORKSTREAM_TASK,
  KIND_ARTIFACT,
  KIND_DECISION_RECORD,
  KIND_TASK_STATUS_CHANGE,
  KIND_ARTIFACT_VERSION,
  KIND_REVIEW_REQUEST,
  KIND_REVIEW_COMMENT,
  KIND_REVIEW_DECISION,
];

/** Heads change on human timescales; poll as a backstop for a missed event. */
const HEAD_STALE_TIME_MS = 30_000;

function invalidateWorkstreamQueries(
  queryClient: ReturnType<typeof useQueryClient>,
) {
  void queryClient.invalidateQueries({
    predicate: (query) =>
      typeof query.queryKey[0] === "string" &&
      query.queryKey[0].startsWith("workstream"),
  });
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

/** All 35000 heads in a channel, newest-first, LWW-reduced. */
export function useWorkstreamsQuery(channelId: string | null) {
  return useQuery({
    queryKey: workstreamsQueryKey(channelId ?? ""),
    queryFn: async ({ queryKey: [, resolvedChannelId] }) => {
      const events = await fetchWorkstreams(resolvedChannelId);
      return resolveHeadList(events)
        .map(parseWorkstream)
        .filter((ws) => ws !== null);
    },
    enabled: channelId !== null && channelId.length > 0,
    staleTime: HEAD_STALE_TIME_MS,
  });
}

/**
 * A single 35000 head resolved by its `d` tag.
 *
 * The detail route carries only the workstream id, so this query is what makes
 * a deep link or a reload work — it never depends on the list view having
 * already loaded, or on which channel that list was showing.
 */
export function useWorkstreamQuery(workstreamId: string | null) {
  return useQuery({
    queryKey: workstreamQueryKey(workstreamId ?? ""),
    queryFn: async ({ queryKey: [, resolvedId] }) => {
      const events = await fetchWorkstreamById(resolvedId);
      const head = resolveHeadList(events)[0];
      return head === undefined ? null : parseWorkstream(head);
    },
    enabled: workstreamId !== null && workstreamId.length > 0,
    staleTime: HEAD_STALE_TIME_MS,
  });
}

export type WorkstreamChildren = {
  tasks: WorkstreamTask[];
  artifacts: Artifact[];
  decisions: DecisionRecord[];
};

const EMPTY_CHILDREN: WorkstreamChildren = {
  tasks: [],
  artifacts: [],
  decisions: [],
};

/**
 * Tasks, artifacts, and decisions for one workstream — one fetch, split by
 * kind after the shared LWW reduce.
 */
export function useWorkstreamChildrenQuery(address: string | null) {
  return useQuery({
    queryKey: workstreamChildrenQueryKey(address ?? ""),
    queryFn: async ({ queryKey: [, resolvedAddress] }) => {
      const events = await fetchWorkstreamChildren(resolvedAddress);
      const heads = resolveHeadList(events);
      const children: WorkstreamChildren = {
        tasks: [],
        artifacts: [],
        decisions: [],
      };

      for (const event of heads) {
        if (event.kind === KIND_WORKSTREAM_TASK) {
          const task = parseTask(event);
          if (task !== null) children.tasks.push(task);
        } else if (event.kind === KIND_ARTIFACT) {
          const artifact = parseArtifact(event);
          if (artifact !== null) children.artifacts.push(artifact);
        } else if (event.kind === KIND_DECISION_RECORD) {
          const decision = parseDecision(event);
          if (decision !== null) children.decisions.push(decision);
        }
      }

      return children;
    },
    enabled: address !== null && address.length > 0,
    staleTime: HEAD_STALE_TIME_MS,
    placeholderData: EMPTY_CHILDREN,
  });
}

/**
 * Append-only history for a workstream's children — artifact versions, review
 * requests, and review decisions, keyed by the child addresses.
 */
export function useWorkstreamHistoryQuery(
  address: string | null,
  childAddresses: readonly string[],
) {
  // Sorted + joined so the key is stable against child-array reordering; an
  // unstable key would refetch the history on every parent render.
  const addressKey = React.useMemo(
    () => [...childAddresses].sort().join(","),
    [childAddresses],
  );

  return useQuery({
    queryKey: [...workstreamHistoryQueryKey(address ?? ""), addressKey],
    queryFn: () => fetchWorkstreamHistory(childAddresses),
    enabled: address !== null && childAddresses.length > 0,
    staleTime: HEAD_STALE_TIME_MS,
  });
}

/** NIP-10 comments threaded under the given review requests. */
export function useReviewCommentsQuery(
  address: string | null,
  requestEventIds: readonly string[],
) {
  const idKey = React.useMemo(
    () => [...requestEventIds].sort().join(","),
    [requestEventIds],
  );

  return useQuery({
    queryKey: [...reviewCommentsQueryKey(address ?? ""), idKey],
    queryFn: () => fetchReviewComments(requestEventIds),
    enabled: address !== null && requestEventIds.length > 0,
    staleTime: 10_000,
  });
}

// ---------------------------------------------------------------------------
// Live updates
// ---------------------------------------------------------------------------

/**
 * Subscribe to workstream-family events in a channel and invalidate on any
 * arrival, so remote edits reach the board without a manual refresh.
 *
 * `limit: 0` requests live-only delivery — the queries above already own
 * backfill, and a replayed history burst here would invalidate them once per
 * event for no benefit.
 */
export function useWorkstreamLiveUpdates(channelId: string | null): void {
  const queryClient = useQueryClient();

  React.useEffect(() => {
    if (channelId === null || channelId.length === 0) return;

    let disposed = false;
    let dispose: (() => void) | undefined;

    const refresh = () => {
      invalidateWorkstreamQueries(queryClient);
    };

    void relayClient
      .subscribeLive(
        { kinds: WORKSTREAM_LIVE_KINDS, "#h": [channelId], limit: 0 },
        refresh,
      )
      .then((unsubscribe) => {
        if (disposed) {
          void unsubscribe();
        } else {
          dispose = () => void unsubscribe();
        }
      })
      .catch((error) => {
        console.error("Failed to subscribe to workstream updates", error);
      });

    // Events published while the socket was down never replay through the
    // live subscription, so a reconnect has to trigger a catch-up fetch.
    const unsubReconnect = relayClient.subscribeToReconnects(refresh);

    return () => {
      disposed = true;
      unsubReconnect();
      dispose?.();
    };
  }, [channelId, queryClient]);
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/**
 * Every mutation invalidates the whole workstream key space rather than
 * patching a cache entry. A status change writes two events across two
 * queries (history and heads), and a decision supersession touches a record
 * the mutation never named — enumerating those relationships in `onSuccess`
 * would be a second, drift-prone copy of the data model.
 */
function useWorkstreamMutation<TInput, TResult>(
  mutationFn: (input: TInput) => Promise<TResult>,
) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn,
    onSuccess: () => invalidateWorkstreamQueries(queryClient),
  });
}

export function useSaveWorkstreamMutation() {
  return useWorkstreamMutation(saveWorkstream);
}

export function useSaveTaskMutation() {
  return useWorkstreamMutation(saveTask);
}

export function useSaveArtifactMutation() {
  return useWorkstreamMutation(saveArtifact);
}

export function useSaveDecisionMutation() {
  return useWorkstreamMutation(saveDecision);
}

export function useChangeTaskStatusMutation() {
  return useWorkstreamMutation(changeTaskStatus);
}

export function useRequestReviewMutation() {
  return useWorkstreamMutation(requestReview);
}

export function useCommentOnReviewMutation() {
  return useWorkstreamMutation(commentOnReview);
}

export function useDecideReviewMutation() {
  return useWorkstreamMutation(decideReview);
}

export type { RelayEvent };
