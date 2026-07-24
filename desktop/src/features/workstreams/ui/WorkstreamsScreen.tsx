import * as React from "react";

import type { Channel } from "@/shared/api/types";
import { ViewLoadingFallback } from "@/shared/ui/ViewLoadingFallback";

const WorkstreamsView = React.lazy(async () => {
  const module = await import("@/features/workstreams/ui/WorkstreamsView");
  return { default: module.WorkstreamsView };
});

const WorkstreamDetailScreen = React.lazy(async () => {
  const module = await import(
    "@/features/workstreams/ui/WorkstreamDetailScreen"
  );
  return { default: module.WorkstreamDetailScreen };
});

type WorkstreamsScreenProps = {
  channels: Channel[];
  onCloseWorkstream: () => void;
  onSelectWorkstream: (workstreamId: string) => void;
  selectedWorkstreamId: string | null;
};

/**
 * Routes between the channel-scoped list and a single workstream's detail.
 * The two are separate screens rather than one component with a mode flag,
 * because they sit on separate routes and load their data independently.
 */
export function WorkstreamsScreen({
  channels,
  onCloseWorkstream,
  onSelectWorkstream,
  selectedWorkstreamId,
}: WorkstreamsScreenProps) {
  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
      <React.Suspense fallback={<ViewLoadingFallback kind="workstreams" />}>
        {selectedWorkstreamId === null ? (
          <WorkstreamsView
            channels={channels}
            onSelectWorkstream={onSelectWorkstream}
          />
        ) : (
          <WorkstreamDetailScreen
            onBack={onCloseWorkstream}
            workstreamId={selectedWorkstreamId}
          />
        )}
      </React.Suspense>
    </div>
  );
}
