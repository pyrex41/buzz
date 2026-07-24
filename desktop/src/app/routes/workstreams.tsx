import * as React from "react";
import { createFileRoute } from "@tanstack/react-router";

import { usePreviewFeatureWarning } from "@/shared/features";
import { ViewLoadingFallback } from "@/shared/ui/ViewLoadingFallback";

export const Route = createFileRoute("/workstreams")({
  component: WorkstreamsRouteComponent,
});

const WorkstreamsRouteScreen = React.lazy(async () => {
  const module = await import("./WorkstreamsRouteScreen");
  return { default: module.WorkstreamsRouteScreen };
});

function WorkstreamsRouteComponent() {
  usePreviewFeatureWarning("workstreams");
  return (
    <React.Suspense fallback={<ViewLoadingFallback kind="workstreams" />}>
      <WorkstreamsRouteScreen selectedWorkstreamId={null} />
    </React.Suspense>
  );
}
