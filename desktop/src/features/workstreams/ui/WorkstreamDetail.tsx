import * as React from "react";
import { ArrowLeft } from "lucide-react";

import { KIND_REVIEW_REQUEST } from "@/shared/constants/kinds";
import { Button } from "@/shared/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/shared/ui/tabs";

import {
  useChangeTaskStatusMutation,
  useRequestReviewMutation,
  useReviewCommentsQuery,
  useSaveTaskMutation,
  useWorkstreamChildrenQuery,
  useWorkstreamHistoryQuery,
} from "../hooks";
import type { Artifact, Workstream, WorkstreamTask } from "../lib/parse";
import { entityIdFromName, type TaskStatus } from "../lib/vocab";
import { ArtifactList } from "./ArtifactList";
import { CreateTaskDialog, type CreateTaskInput } from "./CreateTaskDialog";
import { DecisionLog } from "./DecisionLog";
import { TaskBoard } from "./TaskBoard";
import { WorkstreamStatusChip, WsTypeChip } from "./StatusChip";

type WorkstreamDetailProps = {
  onBack: () => void;
  workstream: Workstream;
};

export function WorkstreamDetail({
  onBack,
  workstream,
}: WorkstreamDetailProps) {
  const [createTaskOpen, setCreateTaskOpen] = React.useState(false);

  const childrenQuery = useWorkstreamChildrenQuery(workstream.address);
  const children = childrenQuery.data ?? {
    tasks: [],
    artifacts: [],
    decisions: [],
  };

  // History is keyed by the child addresses, so it refetches when a child is
  // added but not on every unrelated re-render.
  const childAddresses = React.useMemo(
    () => [
      ...children.tasks.map((task) => task.address),
      ...children.artifacts.map((artifact) => artifact.address),
      ...children.decisions.map((decision) => decision.address),
    ],
    [children.artifacts, children.decisions, children.tasks],
  );

  const historyQuery = useWorkstreamHistoryQuery(
    workstream.address,
    childAddresses,
  );
  const historyEvents = React.useMemo(
    () => historyQuery.data ?? [],
    [historyQuery.data],
  );

  const requestEventIds = React.useMemo(
    () =>
      historyEvents
        .filter((event) => event.kind === KIND_REVIEW_REQUEST)
        .map((event) => event.id),
    [historyEvents],
  );
  const commentsQuery = useReviewCommentsQuery(
    workstream.address,
    requestEventIds,
  );

  const saveTask = useSaveTaskMutation();
  const changeTaskStatus = useChangeTaskStatusMutation();
  const requestReview = useRequestReviewMutation();

  // Depend on the stable mutate methods, never the mutation objects — a
  // React Query result is a fresh object every render (CLAUDE.md gotcha 7).
  const saveTaskAsync = saveTask.mutateAsync;
  const changeTaskStatusAsync = changeTaskStatus.mutateAsync;
  const requestReviewAsync = requestReview.mutateAsync;

  const handleCreateTask = React.useCallback(
    async (input: CreateTaskInput) => {
      await saveTaskAsync({
        id: entityIdFromName(input.name),
        workstream: workstream.coordinate,
        status: input.status,
        name: input.name,
        description: "",
        channels: workstream.channels,
        due: input.due,
      });
    },
    [saveTaskAsync, workstream.channels, workstream.coordinate],
  );

  const handleChangeStatus = React.useCallback(
    (task: WorkstreamTask, status: TaskStatus) => {
      void changeTaskStatusAsync({
        task: task.coordinate,
        status,
        previousStatus: task.status,
        note: "",
        channels: task.channels,
        head: {
          id: task.id,
          workstream: workstream.coordinate,
          status,
          name: task.name,
          description: task.content,
          channels: task.channels,
          assignee: task.assignee,
          due: task.due,
          previousCreatedAt: task.createdAt,
        },
      });
    },
    [changeTaskStatusAsync, workstream.coordinate],
  );

  const handleRequestReview = React.useCallback(
    (artifact: Artifact) => {
      void requestReviewAsync({
        target: artifact.coordinate,
        content: `Please review ${artifact.name}.`,
        channels: artifact.channels,
      });
    },
    [requestReviewAsync],
  );

  const isBusy = saveTask.isPending || changeTaskStatus.isPending;

  return (
    <div
      className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 p-4"
      data-testid="workstream-detail"
    >
      <header className="flex flex-wrap items-center gap-2">
        <Button
          data-testid="workstream-back"
          onClick={onBack}
          size="icon"
          type="button"
          variant="ghost"
        >
          <ArrowLeft className="h-4 w-4" />
          <span className="sr-only">Back to workstreams</span>
        </Button>
        <h1
          className="text-base font-semibold text-foreground"
          data-testid="workstream-detail-name"
        >
          {workstream.name}
        </h1>
        <WsTypeChip wsType={workstream.wsType} />
        <WorkstreamStatusChip status={workstream.status} />
      </header>

      {workstream.content.length > 0 ? (
        <p className="text-sm text-muted-foreground">{workstream.content}</p>
      ) : null}

      <Tabs className="flex min-h-0 flex-1 flex-col" defaultValue="board">
        <div className="flex flex-wrap items-center gap-2">
          <TabsList data-testid="workstream-tabs">
            <TabsTrigger data-testid="workstream-tab-board" value="board">
              Tasks
            </TabsTrigger>
            <TabsTrigger
              data-testid="workstream-tab-artifacts"
              value="artifacts"
            >
              Artifacts
            </TabsTrigger>
            <TabsTrigger
              data-testid="workstream-tab-decisions"
              value="decisions"
            >
              Decisions
            </TabsTrigger>
          </TabsList>
          <Button
            className="ml-auto"
            data-testid="create-task-open"
            disabled={isBusy}
            onClick={() => setCreateTaskOpen(true)}
            size="sm"
            type="button"
          >
            New task
          </Button>
        </div>

        <TabsContent
          className="mt-3 flex min-h-0 flex-1 flex-col"
          value="board"
        >
          <TaskBoard
            isBusy={isBusy}
            onChangeStatus={handleChangeStatus}
            tasks={children.tasks}
          />
        </TabsContent>

        <TabsContent
          className="mt-3 min-h-0 flex-1 overflow-y-auto"
          value="artifacts"
        >
          <ArtifactList
            artifacts={children.artifacts}
            commentEvents={commentsQuery.data ?? []}
            historyEvents={historyEvents}
            isBusy={requestReview.isPending}
            onRequestReview={handleRequestReview}
          />
        </TabsContent>

        <TabsContent
          className="mt-3 min-h-0 flex-1 overflow-y-auto"
          value="decisions"
        >
          <DecisionLog decisions={children.decisions} />
        </TabsContent>
      </Tabs>

      <CreateTaskDialog
        isCreating={saveTask.isPending}
        onCreate={handleCreateTask}
        onOpenChange={setCreateTaskOpen}
        open={createTaskOpen}
      />
    </div>
  );
}
