import * as React from "react";

import type { RelayEvent } from "@/shared/api/types";
import { PubKey } from "@/shared/ui/PubKey";

import type { ReviewDecision, ReviewRequest } from "../lib/parse";
import { parseReviewComment } from "../lib/parse";
import { ReviewVerdictChip } from "./StatusChip";

type ReviewThreadProps = {
  comments: readonly RelayEvent[];
  decisions: readonly ReviewDecision[];
  requests: readonly ReviewRequest[];
  /** The subject's `d` id — only used to build stable test ids. */
  subjectId: string;
};

/**
 * One review cycle: the 47010 request, its NIP-10 threaded 47011 comments,
 * and any 47012 verdict.
 *
 * A verdict is attributed to a specific request via its `e` tag, so a subject
 * reviewed twice shows two independent cycles rather than one verdict
 * apparently answering both.
 */
export function ReviewThread({
  comments,
  decisions,
  requests,
  subjectId,
}: ReviewThreadProps) {
  const parsedComments = React.useMemo(
    () => comments.map(parseReviewComment),
    [comments],
  );

  if (requests.length === 0) return null;

  return (
    <div
      className="mt-3 space-y-3 border-t border-border/60 pt-3"
      data-testid={`review-thread-${subjectId}`}
    >
      {requests.map((request) => {
        const threadComments = parsedComments
          .filter((comment) => comment.rootId === request.eventId)
          .sort((a, b) => a.createdAt - b.createdAt);
        const verdict = decisions.find(
          (decision) => decision.requestId === request.eventId,
        );

        return (
          <section
            data-testid={`review-request-${request.eventId}`}
            key={request.eventId}
          >
            <header className="flex flex-wrap items-center gap-2">
              <span className="text-xs font-medium text-foreground">
                Review requested
              </span>
              <PubKey pubkey={request.pubkey} />
              {verdict !== undefined ? (
                <ReviewVerdictChip verdict={verdict.verdict} />
              ) : (
                <span
                  className="text-2xs uppercase tracking-wider text-muted-foreground"
                  data-testid={`review-pending-${request.eventId}`}
                >
                  Pending
                </span>
              )}
            </header>

            {request.content.length > 0 ? (
              <p className="mt-1 text-xs text-muted-foreground">
                {request.content}
              </p>
            ) : null}

            {threadComments.length > 0 ? (
              <ol className="mt-2 space-y-1.5 border-l border-border/60 pl-3">
                {threadComments.map((comment) => (
                  <li
                    className="text-xs text-muted-foreground"
                    data-testid={`review-comment-${comment.eventId}`}
                    key={comment.eventId}
                  >
                    <PubKey pubkey={comment.pubkey} />
                    <span className="ml-1.5">{comment.content}</span>
                  </li>
                ))}
              </ol>
            ) : null}

            {verdict !== undefined && verdict.content.length > 0 ? (
              <p className="mt-2 text-xs text-muted-foreground">
                {verdict.content}
              </p>
            ) : null}
          </section>
        );
      })}
    </div>
  );
}
