import * as React from "react";

import type { Channel } from "@/shared/api/types";
import { Button } from "@/shared/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";

import {
  useSaveWorkstreamMutation,
  useWorkstreamLiveUpdates,
  useWorkstreamsQuery,
} from "../hooks";
import type { Workstream } from "../lib/parse";
import { entityIdFromName } from "../lib/vocab";
import {
  CreateWorkstreamDialog,
  type CreateWorkstreamInput,
} from "./CreateWorkstreamDialog";
import { WorkstreamStatusChip, WsTypeChip } from "./StatusChip";

type WorkstreamsViewProps = {
  channels: Channel[];
  onSelectWorkstream: (workstreamId: string) => void;
};

/**
 * Workstreams are per-channel, mirroring how every other NIP-29-scoped
 * surface in Buzz works: the `h` tag is what gives a workstream its
 * membership and tenant boundary, so "which channel" is the first choice, not
 * a filter applied afterwards.
 */
export function WorkstreamsView({
  channels,
  onSelectWorkstream,
}: WorkstreamsViewProps) {
  const [activeChannelId, setActiveChannelId] = React.useState<string | null>(
    channels[0]?.id ?? null,
  );
  const [createOpen, setCreateOpen] = React.useState(false);

  // Adopt the first channel once the list loads, but never override a choice
  // the user has already made.
  React.useEffect(() => {
    setActiveChannelId((current) =>
      current === null ? (channels[0]?.id ?? null) : current,
    );
  }, [channels]);

  const workstreamsQuery = useWorkstreamsQuery(activeChannelId);
  useWorkstreamLiveUpdates(activeChannelId);

  const saveWorkstream = useSaveWorkstreamMutation();
  const saveWorkstreamAsync = saveWorkstream.mutateAsync;

  const workstreams = React.useMemo(
    () => workstreamsQuery.data ?? [],
    [workstreamsQuery.data],
  );

  const activeChannel = channels.find(
    (channel) => channel.id === activeChannelId,
  );

  const handleCreate = React.useCallback(
    async (input: CreateWorkstreamInput) => {
      if (activeChannelId === null) {
        throw new Error("Pick a channel before creating a workstream.");
      }
      await saveWorkstreamAsync({
        id: entityIdFromName(input.name),
        wsType: input.wsType,
        status: "active",
        name: input.name,
        description: input.description,
        channels: [activeChannelId],
      });
    },
    [activeChannelId, saveWorkstreamAsync],
  );

  return (
    <div
      className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 p-4"
      data-testid="workstreams-view"
    >
      <header className="flex flex-wrap items-center gap-2">
        <h1 className="text-base font-semibold text-foreground">Workstreams</h1>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              data-testid="workstreams-channel-picker"
              size="sm"
              type="button"
              variant="outline"
            >
              {activeChannel ? `#${activeChannel.name}` : "Pick a channel"}
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            {channels.map((channel) => (
              <DropdownMenuItem
                data-testid={`workstreams-channel-${channel.name}`}
                key={channel.id}
                onSelect={() => setActiveChannelId(channel.id)}
              >
                #{channel.name}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
        <Button
          className="ml-auto"
          data-testid="create-workstream-open"
          disabled={activeChannelId === null}
          onClick={() => setCreateOpen(true)}
          size="sm"
          type="button"
        >
          New workstream
        </Button>
      </header>

      {workstreamsQuery.isLoading ? (
        <p
          className="text-sm text-muted-foreground"
          data-testid="workstreams-loading"
        >
          Loading workstreams…
        </p>
      ) : workstreams.length === 0 ? (
        <p
          className="text-sm text-muted-foreground"
          data-testid="workstreams-empty"
        >
          No workstreams in this channel yet. A workstream holds the tasks,
          artifacts, and decisions for one piece of work — code, hardware, data,
          design, or process.
        </p>
      ) : (
        <ul className="flex flex-col gap-2" data-testid="workstreams-list">
          {workstreams.map((workstream) => (
            <WorkstreamRow
              key={workstream.address}
              onSelect={onSelectWorkstream}
              workstream={workstream}
            />
          ))}
        </ul>
      )}

      <CreateWorkstreamDialog
        isCreating={saveWorkstream.isPending}
        onCreate={handleCreate}
        onOpenChange={setCreateOpen}
        open={createOpen}
      />
    </div>
  );
}

function WorkstreamRow({
  onSelect,
  workstream,
}: {
  onSelect: (workstreamId: string) => void;
  workstream: Workstream;
}) {
  return (
    <li>
      <button
        className="flex w-full flex-col gap-1 rounded-xl border border-border/60 bg-background p-3 text-left transition-colors hover:border-muted-foreground/40"
        data-testid={`workstream-row-${workstream.id}`}
        onClick={() => onSelect(workstream.id)}
        type="button"
      >
        <span className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-semibold text-foreground">
            {workstream.name}
          </span>
          <WsTypeChip wsType={workstream.wsType} />
          <WorkstreamStatusChip status={workstream.status} />
        </span>
        {workstream.content.length > 0 ? (
          <span className="line-clamp-2 text-xs text-muted-foreground">
            {workstream.content}
          </span>
        ) : null}
      </button>
    </li>
  );
}
