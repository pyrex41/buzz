import { Badge, type BadgeProps } from "@/shared/ui/badge";

import {
  DECISION_STATUS_LABELS,
  type DecisionStatus,
  REVIEW_VERDICT_LABELS,
  type ReviewVerdict,
  TASK_STATUS_LABELS,
  type TaskStatus,
  WORKSTREAM_STATUS_LABELS,
  type WorkstreamStatus,
  WS_TYPE_LABELS,
  type WsType,
} from "../lib/vocab";

type Variant = BadgeProps["variant"];

/**
 * Status → badge variant. Kept as lookup tables rather than conditionals so
 * every vocabulary member is visibly accounted for, and TypeScript flags the
 * omission if a vocabulary grows.
 */
const WORKSTREAM_VARIANTS: Record<WorkstreamStatus, Variant> = {
  active: "success",
  paused: "warning",
  done: "info",
  archived: "outline",
};

const TASK_VARIANTS: Record<TaskStatus, Variant> = {
  todo: "outline",
  "in-progress": "info",
  blocked: "destructive",
  "in-review": "warning",
  done: "success",
  cancelled: "secondary",
};

const DECISION_VARIANTS: Record<DecisionStatus, Variant> = {
  proposed: "outline",
  accepted: "success",
  rejected: "destructive",
  superseded: "secondary",
};

const VERDICT_VARIANTS: Record<ReviewVerdict, Variant> = {
  approve: "success",
  "request-changes": "warning",
  reject: "destructive",
};

export function WorkstreamStatusChip({ status }: { status: WorkstreamStatus }) {
  return (
    <Badge
      data-testid={`workstream-status-${status}`}
      variant={WORKSTREAM_VARIANTS[status]}
    >
      {WORKSTREAM_STATUS_LABELS[status]}
    </Badge>
  );
}

export function WsTypeChip({ wsType }: { wsType: WsType }) {
  return (
    <Badge data-testid={`workstream-type-${wsType}`} variant="secondary">
      {WS_TYPE_LABELS[wsType]}
    </Badge>
  );
}

export function TaskStatusChip({ status }: { status: TaskStatus }) {
  return (
    <Badge
      data-testid={`task-status-${status}`}
      variant={TASK_VARIANTS[status]}
    >
      {TASK_STATUS_LABELS[status]}
    </Badge>
  );
}

export function DecisionStatusChip({ status }: { status: DecisionStatus }) {
  return (
    <Badge
      data-testid={`decision-status-${status}`}
      variant={DECISION_VARIANTS[status]}
    >
      {DECISION_STATUS_LABELS[status]}
    </Badge>
  );
}

export function ReviewVerdictChip({ verdict }: { verdict: ReviewVerdict }) {
  return (
    <Badge
      data-testid={`review-verdict-${verdict}`}
      variant={VERDICT_VARIANTS[verdict]}
    >
      {REVIEW_VERDICT_LABELS[verdict]}
    </Badge>
  );
}
