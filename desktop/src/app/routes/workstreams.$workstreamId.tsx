import * as React from "react";
import { createFileRoute } from "@tanstack/react-router";

import { usePreviewFeatureWarning } from "@/shared/features";
import { ViewLoadingFallback } from "@/shared/ui/ViewLoadingFallback";

export const Route = createFileRoute("/workstreams/$workstreamId")({
  component: WorkstreamDetailRouteComponent,
});

const WorkstreamsRouteScreen = React.lazy(async () => {
  const module = await import("./WorkstreamsRouteScreen");
  return { default: module.WorkstreamsRouteScreen };
});

function WorkstreamDetailRouteComponent() {
  usePreviewFeatureWarning("workstreams");
  const { workstreamId } = Route.useParams();

  return (
    <React.Suspense fallback={<ViewLoadingFallback kind="workstreams" />}>
      <WorkstreamsRouteScreen selectedWorkstreamId={workstreamId} />
    </React.Suspense>
  );
}
