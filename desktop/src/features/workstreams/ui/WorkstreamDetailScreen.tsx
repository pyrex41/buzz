import { useWorkstreamLiveUpdates, useWorkstreamQuery } from "../hooks";
import { WorkstreamDetail } from "./WorkstreamDetail";

type WorkstreamDetailScreenProps = {
  onBack: () => void;
  workstreamId: string;
};

/**
 * Loads a workstream head by id and renders its detail.
 *
 * Separate from `WorkstreamsView` because the two are reached by different
 * routes: the list mounts at `/workstreams`, the detail at
 * `/workstreams/<id>`, and a route change tears the other one down. Resolving
 * the head here — rather than reading it out of the list's state — is what
 * makes the detail survive that remount, and a cold deep link work at all.
 */
export function WorkstreamDetailScreen({
  onBack,
  workstreamId,
}: WorkstreamDetailScreenProps) {
  const workstreamQuery = useWorkstreamQuery(workstreamId);
  const workstream = workstreamQuery.data ?? null;

  // The workstream's own channel drives the live subscription, so remote edits
  // land regardless of which channel the list was showing.
  useWorkstreamLiveUpdates(workstream?.channels[0] ?? null);

  if (workstreamQuery.isLoading) {
    return (
      <p
        className="p-4 text-sm text-muted-foreground"
        data-testid="workstream-loading"
      >
        Loading workstream…
      </p>
    );
  }

  if (workstream === null) {
    return (
      <div
        className="flex flex-col gap-2 p-4"
        data-testid="workstream-not-found"
      >
        <p className="text-sm text-muted-foreground">
          That workstream is not available — it may have been archived, or it
          lives in a channel you are not a member of.
        </p>
        <button
          className="self-start text-sm text-primary underline"
          data-testid="workstream-not-found-back"
          onClick={onBack}
          type="button"
        >
          Back to workstreams
        </button>
      </div>
    );
  }

  return <WorkstreamDetail onBack={onBack} workstream={workstream} />;
}
