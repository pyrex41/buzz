/**
 * Closed tag vocabularies for the Workstream kind family (Hive plan §5.2).
 *
 * These mirror the enums in `crates/buzz-sdk/src/workstream.rs` — the relay
 * validates `ws-type` and the review verdict at ingest, so a typo here is a
 * rejected publish rather than a silently unreachable category. Keeping the
 * wire values in one module means the pickers, the chips, and the parsers all
 * agree on what a status *is*.
 */

/** `ws-type` tag — selects presentation, never relay behavior. */
export const WS_TYPES = [
  "code",
  "systems",
  "hardware",
  "data",
  "design",
  "process",
  "docs",
  "general",
] as const;
export type WsType = (typeof WS_TYPES)[number];

/** `status` tag on a 35000 workstream head. */
export const WORKSTREAM_STATUSES = [
  "active",
  "paused",
  "done",
  "archived",
] as const;
export type WorkstreamStatus = (typeof WORKSTREAM_STATUSES)[number];

/** `status` tag on a 35001 task head — also the task board's column order. */
export const TASK_STATUSES = [
  "todo",
  "in-progress",
  "blocked",
  "in-review",
  "done",
  "cancelled",
] as const;
export type TaskStatus = (typeof TASK_STATUSES)[number];

/** `status` tag on a 35003 decision record. */
export const DECISION_STATUSES = [
  "proposed",
  "accepted",
  "rejected",
  "superseded",
] as const;
export type DecisionStatus = (typeof DECISION_STATUSES)[number];

/** `decision` tag on a 47012 review decision. */
export const REVIEW_VERDICTS = [
  "approve",
  "request-changes",
  "reject",
] as const;
export type ReviewVerdict = (typeof REVIEW_VERDICTS)[number];

/**
 * Build a `parse` that returns the fallback for anything outside the
 * vocabulary. Unknown values come from a newer client or a hand-rolled event;
 * they must render as *something* rather than crashing the board, so every
 * vocabulary has a designated safe bucket.
 */
function parser<T extends string>(
  vocabulary: readonly T[],
  fallback: T,
): (value: string | undefined) => T {
  const allowed = new Set<string>(vocabulary);
  return (value) =>
    value !== undefined && allowed.has(value) ? (value as T) : fallback;
}

export const parseWsType = parser(WS_TYPES, "general");
export const parseWorkstreamStatus = parser(WORKSTREAM_STATUSES, "active");
export const parseTaskStatus = parser(TASK_STATUSES, "todo");
export const parseDecisionStatus = parser(DECISION_STATUSES, "proposed");
export const parseReviewVerdict = parser(REVIEW_VERDICTS, "request-changes");

/** Whether a raw tag value is exactly a member of the vocabulary. */
export function isWsType(value: string): value is WsType {
  return (WS_TYPES as readonly string[]).includes(value);
}

export function isTaskStatus(value: string): value is TaskStatus {
  return (TASK_STATUSES as readonly string[]).includes(value);
}

/** Human labels. Wire values are kebab-case; these are what the UI shows. */
export const WS_TYPE_LABELS: Record<WsType, string> = {
  code: "Code",
  systems: "Systems",
  hardware: "Hardware",
  data: "Data",
  design: "Design",
  process: "Process",
  docs: "Docs",
  general: "General",
};

export const WORKSTREAM_STATUS_LABELS: Record<WorkstreamStatus, string> = {
  active: "Active",
  paused: "Paused",
  done: "Done",
  archived: "Archived",
};

export const TASK_STATUS_LABELS: Record<TaskStatus, string> = {
  todo: "To do",
  "in-progress": "In progress",
  blocked: "Blocked",
  "in-review": "In review",
  done: "Done",
  cancelled: "Cancelled",
};

export const DECISION_STATUS_LABELS: Record<DecisionStatus, string> = {
  proposed: "Proposed",
  accepted: "Accepted",
  rejected: "Rejected",
  superseded: "Superseded",
};

export const REVIEW_VERDICT_LABELS: Record<ReviewVerdict, string> = {
  approve: "Approved",
  "request-changes": "Changes requested",
  reject: "Rejected",
};

/**
 * Entity-id (`d` tag) shape, matching the SDK's `check_entity_id`. The `:`
 * exclusion is load-bearing: coordinates are `<kind>:<pubkey>:<d>`, so an id
 * containing a colon would parse back ambiguously.
 */
const ENTITY_ID_RE = /^[a-zA-Z0-9._-]{1,64}$/;

export function isValidEntityId(value: string): boolean {
  return ENTITY_ID_RE.test(value);
}

/**
 * Derive a valid `d`-tag id from a human-typed name, suffixed with enough
 * randomness that two people naming a workstream "Thermal chamber" on
 * different machines do not collide onto one coordinate.
 */
export function entityIdFromName(name: string): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^[-._]+|[-._]+$/g, "")
    .slice(0, 48);
  const suffix = Math.random().toString(36).slice(2, 8);
  return slug.length > 0 ? `${slug}-${suffix}` : `ws-${suffix}`;
}
