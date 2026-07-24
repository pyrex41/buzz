import * as React from "react";

import { PubKey } from "@/shared/ui/PubKey";

import { type DecisionRecord, supersessionChain } from "../lib/parse";
import { DecisionStatusChip } from "./StatusChip";

type DecisionLogProps = {
  decisions: readonly DecisionRecord[];
};

/**
 * Decision records (35003) with their supersession chains rendered.
 *
 * Only chain *heads* get a row — a record that something else supersedes is
 * shown as history under its replacement rather than as a separate entry, so
 * the log reads as "what we decided" instead of "everything anyone ever
 * proposed". A record whose predecessor we have not loaded still renders as
 * its own head, which is the right failure mode: showing it beats hiding it.
 */
export function DecisionLog({ decisions }: DecisionLogProps) {
  const { chains } = React.useMemo(() => {
    const byAddress = new Map(
      decisions.map((decision) => [decision.address, decision]),
    );
    const superseded = new Set(
      decisions
        .map((decision) => decision.supersedes)
        .filter((address): address is string => address !== null),
    );

    const heads = decisions.filter(
      (decision) => !superseded.has(decision.address),
    );

    return {
      chains: heads
        .map((head) => ({
          head,
          chain: supersessionChain(head, byAddress),
        }))
        .sort((a, b) => b.head.createdAt - a.head.createdAt),
    };
  }, [decisions]);

  if (decisions.length === 0) {
    return (
      <p
        className="px-1 py-6 text-sm text-muted-foreground"
        data-testid="decision-log-empty"
      >
        No decisions recorded yet. A decision record captures the context, the
        call, and its consequences — so the reasoning outlives the thread.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-3" data-testid="decision-log">
      {chains.map(({ chain, head }) => (
        <article
          className="rounded-xl border border-border/60 bg-background p-3"
          data-testid={`decision-card-${head.id}`}
          key={head.address}
        >
          <header className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold text-foreground">
              {head.name}
            </h3>
            <DecisionStatusChip status={head.status} />
            <span className="ml-auto text-2xs text-muted-foreground">
              <PubKey pubkey={head.pubkey} />
            </span>
          </header>

          {head.content.length > 0 ? (
            <p className="mt-1.5 whitespace-pre-wrap text-sm text-muted-foreground">
              {head.content}
            </p>
          ) : null}

          {chain.length > 1 ? (
            <ol
              className="mt-2.5 space-y-1 border-l border-border/60 pl-3"
              data-testid={`decision-chain-${head.id}`}
            >
              {chain.slice(0, -1).map((ancestor) => (
                <li
                  className="text-xs text-muted-foreground"
                  key={ancestor.address}
                >
                  Supersedes{" "}
                  <span className="font-medium text-foreground">
                    {ancestor.name}
                  </span>
                </li>
              ))}
            </ol>
          ) : null}
        </article>
      ))}
    </div>
  );
}
