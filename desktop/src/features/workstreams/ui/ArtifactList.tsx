import * as React from "react";

import type { RelayEvent } from "@/shared/api/types";
import {
  KIND_ARTIFACT_VERSION,
  KIND_REVIEW_DECISION,
  KIND_REVIEW_REQUEST,
} from "@/shared/constants/kinds";
import { Badge } from "@/shared/ui/badge";
import { Button } from "@/shared/ui/button";

import type { Artifact } from "../lib/parse";
import {
  parseArtifactVersion,
  parseReviewDecision,
  parseReviewRequest,
} from "../lib/parse";
import { ReviewThread } from "./ReviewThread";

type ArtifactListProps = {
  /** Raw 47002 / 47010 / 47012 events for this workstream's children. */
  historyEvents: readonly RelayEvent[];
  /** Raw 47011 comment events, threaded by `e` tag off a request. */
  commentEvents: readonly RelayEvent[];
  artifacts: readonly Artifact[];
  isBusy: boolean;
  onRequestReview: (artifact: Artifact) => void;
};

/**
 * Artifact heads (35002) with their version history (47002) and review cycle
 * (47010 request → 47011 comments → 47012 decision).
 *
 * History is grouped by artifact address here rather than fetched per
 * artifact: the detail view already holds every child's history from one
 * query, so grouping in memory avoids N round trips for an N-artifact
 * workstream.
 */
export function ArtifactList({
  artifacts,
  commentEvents,
  historyEvents,
  isBusy,
  onRequestReview,
}: ArtifactListProps) {
  const versionsByArtifact = React.useMemo(() => {
    const grouped = new Map<
      string,
      ReturnType<typeof parseArtifactVersion>[]
    >();
    for (const event of historyEvents) {
      if (event.kind !== KIND_ARTIFACT_VERSION) continue;
      const version = parseArtifactVersion(event);
      if (version.artifact === null) continue;
      const bucket = grouped.get(version.artifact) ?? [];
      bucket.push(version);
      grouped.set(version.artifact, bucket);
    }
    for (const bucket of grouped.values()) {
      bucket.sort((a, b) => b.createdAt - a.createdAt);
    }
    return grouped;
  }, [historyEvents]);

  const requestsByTarget = React.useMemo(() => {
    const grouped = new Map<string, ReturnType<typeof parseReviewRequest>[]>();
    for (const event of historyEvents) {
      if (event.kind !== KIND_REVIEW_REQUEST) continue;
      const request = parseReviewRequest(event);
      if (request.target === null) continue;
      const bucket = grouped.get(request.target) ?? [];
      bucket.push(request);
      grouped.set(request.target, bucket);
    }
    for (const bucket of grouped.values()) {
      bucket.sort((a, b) => b.createdAt - a.createdAt);
    }
    return grouped;
  }, [historyEvents]);

  const decisions = React.useMemo(
    () =>
      historyEvents
        .filter((event) => event.kind === KIND_REVIEW_DECISION)
        .map(parseReviewDecision),
    [historyEvents],
  );

  if (artifacts.length === 0) {
    return (
      <p
        className="px-1 py-6 text-sm text-muted-foreground"
        data-testid="artifact-list-empty"
      >
        No artifacts yet. Artifacts are the documents, datasets, designs, BOMs,
        and results this work produces.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-3" data-testid="artifact-list">
      {artifacts.map((artifact) => (
        <article
          className="rounded-xl border border-border/60 bg-background p-3"
          data-testid={`artifact-card-${artifact.id}`}
          key={artifact.address}
        >
          <header className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold text-foreground">
              {artifact.name}
            </h3>
            <Badge variant="secondary">{artifact.artifactType}</Badge>
            {artifact.version !== null ? (
              <Badge
                data-testid={`artifact-version-${artifact.id}`}
                variant="outline"
              >
                {artifact.version}
              </Badge>
            ) : null}
            <div className="ml-auto">
              <Button
                data-testid={`artifact-request-review-${artifact.id}`}
                disabled={isBusy}
                onClick={() => onRequestReview(artifact)}
                size="sm"
                type="button"
                variant="outline"
              >
                Request review
              </Button>
            </div>
          </header>

          {artifact.content.length > 0 ? (
            <p className="mt-1.5 text-sm text-muted-foreground">
              {artifact.content}
            </p>
          ) : null}

          <VersionHistory
            versions={versionsByArtifact.get(artifact.address) ?? []}
          />

          <ReviewThread
            comments={commentEvents}
            decisions={decisions}
            requests={requestsByTarget.get(artifact.address) ?? []}
            subjectId={artifact.id}
          />
        </article>
      ))}
    </div>
  );
}

function VersionHistory({
  versions,
}: {
  versions: readonly ReturnType<typeof parseArtifactVersion>[];
}) {
  if (versions.length === 0) return null;

  return (
    <ol className="mt-2.5 space-y-1 border-l border-border/60 pl-3">
      {versions.map((version) => (
        <li className="text-xs text-muted-foreground" key={version.eventId}>
          <span className="font-medium text-foreground">{version.version}</span>
          {version.changelog.length > 0 ? ` — ${version.changelog}` : null}
        </li>
      ))}
    </ol>
  );
}
