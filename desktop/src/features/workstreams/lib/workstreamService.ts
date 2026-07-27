/**
 * Relay reads and writes for the Workstream family.
 *
 * Everything here rides the generic Nostr path — `signRelayEvent` (the seckey
 * stays in Rust) then `relayClient.publishEvent`, and `relayClient.fetchEvents`
 * for reads. No new transport, no new HTTP endpoint; the Hive plan's §5.1
 * point is that these kinds need none.
 *
 * Tag shapes mirror `crates/buzz-sdk/src/workstream.rs` exactly, because the
 * relay's ingest gate (`handlers/workstream.rs`) rejects anything else with an
 * OK-false `invalid: …`.
 */

import { relayClient } from "@/shared/api/relayClient";
import { signRelayEvent } from "@/shared/api/tauri";
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

import { type Coordinate, formatCoordinate, parseCoordinate } from "./heads";
import type {
  DecisionStatus,
  ReviewVerdict,
  TaskStatus,
  WorkstreamStatus,
  WsType,
} from "./vocab";

/**
 * Head fetches are bounded rather than paged. A channel's workstreams, and a
 * workstream's tasks, are human-authored inventories — hundreds, not
 * millions — and the relay already collapses each coordinate to one event, so
 * this is an entity count rather than a history depth.
 */
const HEAD_FETCH_LIMIT = 500;

/** History (47xxx) is append-only, so it genuinely accumulates. */
const HISTORY_FETCH_LIMIT = 500;

/**
 * `created_at` for a replacement.
 *
 * NIP-33 replacement is strictly newer-wins, so republishing a head inside the
 * same wall-clock second as its predecessor is rejected as a duplicate. Taking
 * `max(now, previous + 1)` guarantees forward progress even when two edits
 * land back-to-back or the clock is behind the event we are replacing.
 */
function replacementCreatedAt(previousCreatedAt: number | undefined): number {
  const now = Math.floor(Date.now() / 1_000);
  return previousCreatedAt === undefined
    ? now
    : Math.max(now, previousCreatedAt + 1);
}

/** Append `["h", channelId]` for each channel. Every event needs at least one. */
function channelTags(channels: readonly string[]): string[][] {
  return channels.map((channelId) => ["h", channelId]);
}

async function publish(
  input: {
    kind: number;
    content: string;
    tags: string[][];
    createdAt?: number;
  },
  subject: string,
): Promise<RelayEvent> {
  const event = await signRelayEvent(input);
  return relayClient.publishEvent(
    event,
    `Timed out while saving the ${subject}.`,
    `Failed to save the ${subject}.`,
  );
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/** Every 35000 head visible in a channel. */
export async function fetchWorkstreams(
  channelId: string,
): Promise<RelayEvent[]> {
  return relayClient.fetchEvents({
    kinds: [KIND_WORKSTREAM],
    "#h": [channelId],
    limit: HEAD_FETCH_LIMIT,
  });
}

/**
 * Resolve a single 35000 head by its `d` tag, without knowing the channel.
 *
 * The detail route is `/workstreams/<id>` — no channel in the path — so a
 * deep link, a reload, or a list→detail navigation that remounts the view all
 * have to find the head from the id alone. Querying by `#d` keeps the detail
 * addressable on its own instead of depending on whichever channel the list
 * happened to be showing.
 *
 * The `limit` leaves room for the same `d` under different authors; the
 * caller's LWW reduce picks the winner.
 */
export async function fetchWorkstreamById(
  workstreamId: string,
): Promise<RelayEvent[]> {
  return relayClient.fetchEvents({
    kinds: [KIND_WORKSTREAM],
    "#d": [workstreamId],
    limit: 20,
  });
}

/**
 * Tasks, artifacts, and decision records belonging to one workstream.
 *
 * All three are `a`-tagged at the workstream, so a single filter fetches the
 * detail view's entire head inventory — one round trip instead of three.
 * Artifacts carry the `a` tag optionally in the SDK, so an artifact created
 * outside a workstream simply does not appear here, which is correct.
 */
export async function fetchWorkstreamChildren(
  workstreamAddress: string,
): Promise<RelayEvent[]> {
  return relayClient.fetchEvents({
    kinds: [KIND_WORKSTREAM_TASK, KIND_ARTIFACT, KIND_DECISION_RECORD],
    "#a": [workstreamAddress],
    limit: HEAD_FETCH_LIMIT,
  });
}

/**
 * Append-only history for a set of subjects (`a` coordinates): artifact
 * versions, review requests, and review decisions.
 *
 * Review *comments* are excluded — they thread on `e` tags off a request, so
 * they are fetched per-thread by `fetchReviewComments` rather than by subject.
 */
export async function fetchWorkstreamHistory(
  addresses: readonly string[],
): Promise<RelayEvent[]> {
  if (addresses.length === 0) return [];
  return relayClient.fetchEvents({
    kinds: [
      KIND_TASK_STATUS_CHANGE,
      KIND_ARTIFACT_VERSION,
      KIND_REVIEW_REQUEST,
      KIND_REVIEW_DECISION,
    ],
    "#a": [...addresses],
    limit: HISTORY_FETCH_LIMIT,
  });
}

/** NIP-10 threaded comments hanging off the given review-request events. */
export async function fetchReviewComments(
  requestEventIds: readonly string[],
): Promise<RelayEvent[]> {
  if (requestEventIds.length === 0) return [];
  return relayClient.fetchEvents({
    kinds: [KIND_REVIEW_COMMENT],
    "#e": [...requestEventIds],
    limit: HISTORY_FETCH_LIMIT,
  });
}

// ---------------------------------------------------------------------------
// Writes — addressable heads (35000–35003)
// ---------------------------------------------------------------------------

export type SaveWorkstreamInput = {
  id: string;
  wsType: WsType;
  status: WorkstreamStatus;
  name: string;
  description: string;
  channels: string[];
  members?: string[];
  /** `created_at` of the head being replaced; omit when creating. */
  previousCreatedAt?: number;
};

export async function saveWorkstream(
  input: SaveWorkstreamInput,
): Promise<RelayEvent> {
  const tags: string[][] = [
    ["d", input.id],
    ["ws-type", input.wsType],
    ["status", input.status],
    ["name", input.name],
    ...(input.members ?? []).map((pubkey) => ["p", pubkey]),
    ...channelTags(input.channels),
  ];

  return publish(
    {
      kind: KIND_WORKSTREAM,
      content: input.description,
      createdAt: replacementCreatedAt(input.previousCreatedAt),
      tags,
    },
    "workstream",
  );
}

export type SaveTaskInput = {
  id: string;
  workstream: Coordinate;
  status: TaskStatus;
  name: string;
  description: string;
  channels: string[];
  assignee?: string | null;
  due?: string | null;
  previousCreatedAt?: number;
};

export async function saveTask(input: SaveTaskInput): Promise<RelayEvent> {
  const tags: string[][] = [
    ["d", input.id],
    ["a", formatCoordinate(input.workstream)],
    ["status", input.status],
    ["name", input.name],
  ];
  // The SDK emits assignee twice — once as `assignee` (the semantic slot) and
  // once as `p` (so generic participant filters and notifications find it).
  if (input.assignee) {
    tags.push(["assignee", input.assignee], ["p", input.assignee]);
  }
  if (input.due) tags.push(["due", input.due]);
  tags.push(...channelTags(input.channels));

  return publish(
    {
      kind: KIND_WORKSTREAM_TASK,
      content: input.description,
      createdAt: replacementCreatedAt(input.previousCreatedAt),
      tags,
    },
    "task",
  );
}

export type SaveArtifactInput = {
  id: string;
  artifactType: string;
  name: string;
  description: string;
  workstream: Coordinate | null;
  channels: string[];
  version?: string | null;
  blobs?: string[];
  previousCreatedAt?: number;
};

export async function saveArtifact(
  input: SaveArtifactInput,
): Promise<RelayEvent> {
  const tags: string[][] = [
    ["d", input.id],
    ["artifact-type", input.artifactType],
    ["name", input.name],
  ];
  if (input.workstream !== null) {
    tags.push(["a", formatCoordinate(input.workstream)]);
  }
  if (input.version) tags.push(["version", input.version]);
  for (const blob of input.blobs ?? []) tags.push(["x", blob]);
  tags.push(...channelTags(input.channels));

  return publish(
    {
      kind: KIND_ARTIFACT,
      content: input.description,
      createdAt: replacementCreatedAt(input.previousCreatedAt),
      tags,
    },
    "artifact",
  );
}

export type SaveDecisionInput = {
  id: string;
  workstream: Coordinate;
  status: DecisionStatus;
  name: string;
  body: string;
  channels: string[];
  supersedes?: string | null;
  previousCreatedAt?: number;
};

export async function saveDecision(
  input: SaveDecisionInput,
): Promise<RelayEvent> {
  const tags: string[][] = [
    ["d", input.id],
    ["a", formatCoordinate(input.workstream)],
    ["status", input.status],
    ["name", input.name],
  ];
  if (input.supersedes) tags.push(["supersedes", input.supersedes]);
  tags.push(...channelTags(input.channels));

  return publish(
    {
      kind: KIND_DECISION_RECORD,
      content: input.body,
      createdAt: replacementCreatedAt(input.previousCreatedAt),
      tags,
    },
    "decision record",
  );
}

// ---------------------------------------------------------------------------
// Writes — append-only history (47xxx)
// ---------------------------------------------------------------------------

/**
 * Move a task to a new status.
 *
 * Two events, in this order: the immutable 47001 record of the transition,
 * then the replaced 35001 head. History first means a crash between the two
 * leaves an auditable "someone moved this" trail rather than a silent jump;
 * the reverse order would lose the transition entirely.
 */
export async function changeTaskStatus(input: {
  task: Coordinate;
  status: TaskStatus;
  previousStatus: TaskStatus;
  note: string;
  channels: string[];
  head: SaveTaskInput;
}): Promise<RelayEvent> {
  if (input.status !== input.previousStatus) {
    const tags: string[][] = [
      ["a", formatCoordinate(input.task)],
      ["status", input.status],
      ["previous-status", input.previousStatus],
      ...channelTags(input.channels),
    ];
    await publish(
      {
        kind: KIND_TASK_STATUS_CHANGE,
        content: input.note,
        tags,
      },
      "task status change",
    );
  }

  return saveTask({ ...input.head, status: input.status });
}

export async function publishArtifactVersion(input: {
  artifact: Coordinate;
  version: string;
  contentHash: string;
  changelog: string;
  blobs?: string[];
  channels: string[];
}): Promise<RelayEvent> {
  const tags: string[][] = [
    ["a", formatCoordinate(input.artifact)],
    ["version", input.version],
    ["content-hash", input.contentHash],
    ...(input.blobs ?? []).map((blob) => ["x", blob]),
    ...channelTags(input.channels),
  ];

  return publish(
    { kind: KIND_ARTIFACT_VERSION, content: input.changelog, tags },
    "artifact version",
  );
}

export async function requestReview(input: {
  target: Coordinate;
  content: string;
  reviewers?: string[];
  channels: string[];
}): Promise<RelayEvent> {
  const tags: string[][] = [
    ["a", formatCoordinate(input.target)],
    ...(input.reviewers ?? []).map((pubkey) => ["p", pubkey]),
    ...channelTags(input.channels),
  ];

  return publish(
    { kind: KIND_REVIEW_REQUEST, content: input.content, tags },
    "review request",
  );
}

/**
 * Comment on a review thread. NIP-10 markers match the SDK: a one-deep reply
 * carries a single `reply` marker, deeper replies carry `root` + `reply`, so
 * existing Buzz thread rendering reads these unchanged.
 */
export async function commentOnReview(input: {
  rootEventId: string;
  parentEventId: string;
  target?: Coordinate | null;
  content: string;
  channels: string[];
}): Promise<RelayEvent> {
  const tags: string[][] =
    input.rootEventId === input.parentEventId
      ? [["e", input.rootEventId, "", "reply"]]
      : [
          ["e", input.rootEventId, "", "root"],
          ["e", input.parentEventId, "", "reply"],
        ];
  if (input.target) tags.push(["a", formatCoordinate(input.target)]);
  tags.push(...channelTags(input.channels));

  return publish(
    { kind: KIND_REVIEW_COMMENT, content: input.content, tags },
    "review comment",
  );
}

export async function decideReview(input: {
  target: Coordinate;
  requestEventId: string;
  verdict: ReviewVerdict;
  content: string;
  channels: string[];
}): Promise<RelayEvent> {
  const tags: string[][] = [
    ["a", formatCoordinate(input.target)],
    ["e", input.requestEventId, "", "reply"],
    ["decision", input.verdict],
    ...channelTags(input.channels),
  ];

  return publish(
    { kind: KIND_REVIEW_DECISION, content: input.content, tags },
    "review decision",
  );
}

/** Convenience re-export so UI modules import coordinates from one place. */
export { formatCoordinate, parseCoordinate };
export type { Coordinate };
