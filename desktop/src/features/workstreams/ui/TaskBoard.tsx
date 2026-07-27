import * as React from "react";

import { PubKey } from "@/shared/ui/PubKey";
import { Button } from "@/shared/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";

import type { WorkstreamTask } from "../lib/parse";
import {
  TASK_STATUSES,
  TASK_STATUS_LABELS,
  type TaskStatus,
} from "../lib/vocab";

type TaskBoardProps = {
  isBusy: boolean;
  onChangeStatus: (task: WorkstreamTask, status: TaskStatus) => void;
  tasks: readonly WorkstreamTask[];
};

/**
 * Columns are the task-status vocabulary in declaration order, which reads
 * left-to-right as the lifecycle. Every column renders even when empty — a
 * board that hides "Blocked" until something is blocked makes the blocked
 * state easy to miss, which is the opposite of what a board is for.
 */
export function TaskBoard({ isBusy, onChangeStatus, tasks }: TaskBoardProps) {
  const byStatus = React.useMemo(() => {
    const grouped = new Map<TaskStatus, WorkstreamTask[]>(
      TASK_STATUSES.map((status) => [status, []]),
    );
    for (const task of tasks) grouped.get(task.status)?.push(task);
    return grouped;
  }, [tasks]);

  return (
    <div
      className="flex min-h-0 flex-1 gap-3 overflow-x-auto pb-2"
      data-testid="task-board"
    >
      {TASK_STATUSES.map((status) => {
        const column = byStatus.get(status) ?? [];
        return (
          <section
            className="flex w-64 shrink-0 flex-col rounded-xl bg-muted/40 p-2"
            data-testid={`task-column-${status}`}
            key={status}
          >
            <header className="flex items-center justify-between px-1 pb-2">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {TASK_STATUS_LABELS[status]}
              </h3>
              <span
                className="text-2xs text-muted-foreground"
                data-testid={`task-column-count-${status}`}
              >
                {column.length}
              </span>
            </header>
            <div className="flex min-h-0 flex-1 flex-col gap-2 overflow-y-auto">
              {column.map((task) => (
                <TaskCard
                  isBusy={isBusy}
                  key={task.address}
                  onChangeStatus={onChangeStatus}
                  task={task}
                />
              ))}
              {column.length === 0 ? (
                <p className="px-1 py-2 text-xs text-muted-foreground/70">
                  Nothing here.
                </p>
              ) : null}
            </div>
          </section>
        );
      })}
    </div>
  );
}

type TaskCardProps = {
  isBusy: boolean;
  onChangeStatus: (task: WorkstreamTask, status: TaskStatus) => void;
  task: WorkstreamTask;
};

function TaskCard({ isBusy, onChangeStatus, task }: TaskCardProps) {
  return (
    <article
      className="rounded-lg border border-border/60 bg-background p-2.5 shadow-sm"
      data-testid={`task-card-${task.id}`}
    >
      <p className="text-sm font-medium leading-snug text-foreground">
        {task.name}
      </p>
      {task.assignee !== null || task.due !== null ? (
        <div className="mt-2 flex flex-wrap items-center gap-2 text-2xs text-muted-foreground">
          {task.assignee !== null ? (
            <span
              className="inline-flex items-center gap-1"
              data-testid={`task-assignee-${task.id}`}
            >
              <PubKey pubkey={task.assignee} />
            </span>
          ) : null}
          {task.due !== null ? (
            <span data-testid={`task-due-${task.id}`}>Due {task.due}</span>
          ) : null}
        </div>
      ) : null}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            className="mt-2 h-6 w-full justify-start px-1.5 text-2xs text-muted-foreground"
            data-testid={`task-move-${task.id}`}
            disabled={isBusy}
            size="sm"
            type="button"
            variant="ghost"
          >
            Move…
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          {TASK_STATUSES.filter((status) => status !== task.status).map(
            (status) => (
              <DropdownMenuItem
                data-testid={`task-move-${task.id}-${status}`}
                key={status}
                onSelect={() => onChangeStatus(task, status)}
              >
                {TASK_STATUS_LABELS[status]}
              </DropdownMenuItem>
            ),
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </article>
  );
}
