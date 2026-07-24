/**
 * Event → domain projections for the Workstream family.
 *
 * Every parser is total: an event that reached us passed relay validation, but
 * a *newer* client may have written tags this build does not know, so unknown
 * vocabulary values fall back (see `vocab.ts`) rather than dropping the row.
 * A workstream that renders with a wrong-looking status is recoverable; one
 * that vanishes from the list is not.
 */

import type { RelayEvent } from "@/shared/api/types";

import {
  type Coordinate,
  coordinateOf,
  formatCoordinate,
  parseCoordinate,
  tagValue,
  tagValues,
} from "./heads";
import {
  type DecisionStatus,
  type ReviewVerdict,
  type TaskStatus,
  type WorkstreamStatus,
  type WsType,
  parseDecisionStatus,
  parseReviewVerdict,
  parseTaskStatus,
  parseWorkstreamStatus,
  parseWsType,
} from "./vocab";

/** Fields every addressable head shares. */
type HeadBase = {
  /** Rendered `<kind>:<pubkey>:<d>` — the stable React key and lookup key. */
  address: string;
  coordinate: Coordinate;
  /** The `d` tag. */
  id: string;
  eventId: string;
  pubkey: string;
  createdAt: number;
  name: string;
  content: string;
  channels: string[];
};

export type Workstream = HeadBase & {
  wsType: WsType;
  status: WorkstreamStatus;
  members: string[];
};

export type WorkstreamTask = HeadBase & {
  status: TaskStatus;
  workstream: string | null;
  assignee: string | null;
  due: string | null;
};

export type Artifact = HeadBase & {
  artifactType: string;
  workstream: string | null;
  version: string | null;
  blobs: string[];
};

export type DecisionRecord = HeadBase & {
  status: DecisionStatus;
  workstream: string | null;
  supersedes: string | null;
};

export type ArtifactVersion = {
  eventId: string;
  pubkey: string;
  createdAt: number;
  artifact: string | null;
  version: string;
  contentHash: string | null;
  changelog: string;
};

export type ReviewRequest = {
  eventId: string;
  pubkey: string;
  createdAt: number;
  target: string | null;
  reviewers: string[];
  content: string;
};

export type ReviewComment = {
  eventId: string;
  pubkey: string;
  createdAt: number;
  /** NIP-10 root marker, or the reply target when the thread is one deep. */
  rootId: string | null;
  parentId: string | null;
  content: string;
};

export type ReviewDecision = {
  eventId: string;
  pubkey: string;
  createdAt: number;
  target: string | null;
  requestId: string | null;
  verdict: ReviewVerdict;
  content: string;
};

export type TaskStatusChange = {
  eventId: string;
  pubkey: string;
  createdAt: number;
  task: string | null;
  status: TaskStatus;
  previousStatus: TaskStatus | null;
  note: string;
};

/** Shared head fields. Returns null when the event is not addressable. */
function headBase(event: RelayEvent): HeadBase | null {
  const coordinate = coordinateOf(event);
  if (coordinate === null) return null;

  return {
    address: formatCoordinate(coordinate),
    coordinate,
    id: coordinate.id,
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    name: tagValue(event, "name") ?? coordinate.id,
    content: event.content,
    channels: tagValues(event, "h"),
  };
}

/**
 * Normalize an `a` tag to a coordinate string, dropping references whose kind
 * is wrong for the slot. A task pointing its `a` at an artifact is a
 * mis-issued event, and following it would file the task under a parent that
 * cannot hold it.
 */
function linkedAddress(event: RelayEvent, expectedKind: number): string | null {
  for (const value of tagValues(event, "a")) {
    const coord = parseCoordinate(value);
    if (coord !== null && coord.kind === expectedKind) {
      return formatCoordinate(coord);
    }
  }
  return null;
}

export function parseWorkstream(event: RelayEvent): Workstream | null {
  const base = headBase(event);
  if (base === null) return null;

  return {
    ...base,
    wsType: parseWsType(tagValue(event, "ws-type")),
    status: parseWorkstreamStatus(tagValue(event, "status")),
    members: tagValues(event, "p"),
  };
}

export function parseTask(event: RelayEvent): WorkstreamTask | null {
  const base = headBase(event);
  if (base === null) return null;

  return {
    ...base,
    status: parseTaskStatus(tagValue(event, "status")),
    workstream: linkedAddress(event, 35000),
    assignee: tagValue(event, "assignee") ?? null,
    due: tagValue(event, "due") ?? null,
  };
}

export function parseArtifact(event: RelayEvent): Artifact | null {
  const base = headBase(event);
  if (base === null) return null;

  return {
    ...base,
    artifactType: tagValue(event, "artifact-type") ?? "doc",
    workstream: linkedAddress(event, 35000),
    version: tagValue(event, "version") ?? null,
    blobs: tagValues(event, "x"),
  };
}

export function parseDecision(event: RelayEvent): DecisionRecord | null {
  const base = headBase(event);
  if (base === null) return null;

  const supersedesRaw = tagValue(event, "supersedes");
  const supersedesCoord =
    supersedesRaw !== undefined ? parseCoordinate(supersedesRaw) : null;

  return {
    ...base,
    status: parseDecisionStatus(tagValue(event, "status")),
    workstream: linkedAddress(event, 35000),
    supersedes:
      supersedesCoord !== null ? formatCoordinate(supersedesCoord) : null,
  };
}

export function parseArtifactVersion(event: RelayEvent): ArtifactVersion {
  return {
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    artifact: linkedAddress(event, 35002),
    version: tagValue(event, "version") ?? "—",
    contentHash: tagValue(event, "content-hash") ?? null,
    changelog: event.content,
  };
}

export function parseReviewRequest(event: RelayEvent): ReviewRequest {
  return {
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    target: tagValues(event, "a")[0] ?? null,
    reviewers: tagValues(event, "p"),
    content: event.content,
  };
}

/**
 * NIP-10 marked `e` tags. A one-deep reply carries a single `["e", id, "",
 * "reply"]`, so root and parent collapse to the same id — matching how the SDK
 * writes them and how existing Buzz thread rendering reads them.
 */
export function parseReviewComment(event: RelayEvent): ReviewComment {
  let rootId: string | null = null;
  let parentId: string | null = null;

  for (const tag of event.tags) {
    if (tag[0] !== "e" || tag[1] === undefined) continue;
    const marker = tag[3];
    if (marker === "root") rootId = tag[1];
    else if (marker === "reply") parentId = tag[1];
    else if (parentId === null) parentId = tag[1];
  }

  return {
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    rootId: rootId ?? parentId,
    parentId: parentId ?? rootId,
    content: event.content,
  };
}

export function parseReviewDecision(event: RelayEvent): ReviewDecision {
  const requestId = event.tags.find(
    (tag) => tag[0] === "e" && tag[1] !== undefined,
  )?.[1];

  return {
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    target: tagValues(event, "a")[0] ?? null,
    requestId: requestId ?? null,
    verdict: parseReviewVerdict(tagValue(event, "decision")),
    content: event.content,
  };
}

export function parseTaskStatusChange(event: RelayEvent): TaskStatusChange {
  const previous = tagValue(event, "previous-status");

  return {
    eventId: event.id,
    pubkey: event.pubkey,
    createdAt: event.created_at,
    task: linkedAddress(event, 35001),
    status: parseTaskStatus(tagValue(event, "status")),
    previousStatus: previous !== undefined ? parseTaskStatus(previous) : null,
    note: event.content,
  };
}

/**
 * Order a decision-supersession chain oldest → newest.
 *
 * Each record points *backwards* at the one it replaces, so the chain is
 * reconstructed by following `supersedes` links. A cycle (two records naming
 * each other, which no honest client writes but a hostile one could) is broken
 * by the visited set rather than hanging the render.
 */
export function supersessionChain(
  decision: DecisionRecord,
  byAddress: ReadonlyMap<string, DecisionRecord>,
): DecisionRecord[] {
  const chain: DecisionRecord[] = [];
  const visited = new Set<string>();
  let cursor: DecisionRecord | undefined = decision;

  while (cursor !== undefined && !visited.has(cursor.address)) {
    visited.add(cursor.address);
    chain.push(cursor);
    cursor =
      cursor.supersedes !== null ? byAddress.get(cursor.supersedes) : undefined;
  }

  return chain.reverse();
}
