/**
 * NIP-33 addressable head resolution.
 *
 * Kinds 35000–35003 are parameterized-replaceable: the relay keeps only the
 * latest event per `(kind, pubkey, d)` coordinate, so in steady state a query
 * returns one event per head. Clients cannot rely on that, though — a live
 * subscription delivers a replacement *alongside* the copy already in hand,
 * reconnect replay re-delivers history, and two relays in a mesh can hand us
 * both sides of a race. So the client reduces to the same winner the relay
 * would pick, rather than trusting arrival order.
 *
 * The rule is NIP-01's: highest `created_at` wins; ties break to the
 * lexicographically **smallest** event id. Both inputs are signed event data,
 * which makes the reduce a pure function of the event *set* — feed it the same
 * events in any order and it yields the same heads.
 */

import type { RelayEvent } from "@/shared/api/types";

/** A parsed `<kind>:<pubkey>:<d>` address. */
export type Coordinate = {
  kind: number;
  pubkey: string;
  id: string;
};

/** Render a coordinate as its `a`-tag value. */
export function formatCoordinate(coord: Coordinate): string {
  return `${coord.kind}:${coord.pubkey}:${coord.id}`;
}

/**
 * Parse an `a`-tag value. Returns null for anything malformed — a bad
 * coordinate is a reference we cannot follow, not a reason to throw inside a
 * render pass.
 *
 * Splits on the first two colons only, so a `d` containing a colon (which the
 * SDK forbids, but a foreign client may not) round-trips into `id` rather than
 * silently truncating.
 */
export function parseCoordinate(value: string): Coordinate | null {
  const firstColon = value.indexOf(":");
  if (firstColon <= 0) return null;
  const secondColon = value.indexOf(":", firstColon + 1);
  if (secondColon < 0) return null;

  const kind = Number(value.slice(0, firstColon));
  if (!Number.isInteger(kind) || kind < 0) return null;

  const pubkey = value.slice(firstColon + 1, secondColon);
  const id = value.slice(secondColon + 1);
  if (pubkey.length === 0 || id.length === 0) return null;

  return { kind, pubkey, id };
}

/** First value of the named tag, or undefined. */
export function tagValue(
  event: Pick<RelayEvent, "tags">,
  name: string,
): string | undefined {
  for (const tag of event.tags) {
    if (tag[0] === name && tag[1] !== undefined) return tag[1];
  }
  return undefined;
}

/** Every value of the named tag, in event order. */
export function tagValues(
  event: Pick<RelayEvent, "tags">,
  name: string,
): string[] {
  const values: string[] = [];
  for (const tag of event.tags) {
    if (tag[0] === name && tag[1] !== undefined) values.push(tag[1]);
  }
  return values;
}

/** The coordinate an addressable event addresses itself as. */
export function coordinateOf(event: RelayEvent): Coordinate | null {
  const d = tagValue(event, "d");
  if (d === undefined || d.length === 0) return null;
  return { kind: event.kind, pubkey: event.pubkey, id: d };
}

/**
 * True when `candidate` supersedes `incumbent` under NIP-01 replaceable
 * ordering. Exported because the live-subscription path applies the same test
 * to a single arriving event without rebuilding the whole map.
 */
export function supersedes(
  candidate: RelayEvent,
  incumbent: RelayEvent,
): boolean {
  if (candidate.created_at !== incumbent.created_at) {
    return candidate.created_at > incumbent.created_at;
  }
  return candidate.id < incumbent.id;
}

/**
 * Reduce a bag of addressable events to one winner per coordinate.
 *
 * Events without a usable `d` tag are dropped: they are not addressable, so
 * they have no coordinate to win. The returned map is keyed by the rendered
 * coordinate string.
 */
export function resolveHeads(
  events: readonly RelayEvent[],
): Map<string, RelayEvent> {
  const heads = new Map<string, RelayEvent>();

  for (const event of events) {
    const coord = coordinateOf(event);
    if (coord === null) continue;

    const key = formatCoordinate(coord);
    const incumbent = heads.get(key);
    if (incumbent === undefined || supersedes(event, incumbent)) {
      heads.set(key, event);
    }
  }

  return heads;
}

/**
 * `resolveHeads` as a list, newest-first. Sorting inside the resolver keeps
 * every caller's ordering identical, which matters because the list view and
 * the board read from the same reduce.
 */
export function resolveHeadList(events: readonly RelayEvent[]): RelayEvent[] {
  return [...resolveHeads(events).values()].sort((a, b) =>
    a.created_at !== b.created_at
      ? b.created_at - a.created_at
      : a.id < b.id
        ? -1
        : 1,
  );
}

/**
 * Merge newly-arrived events into an existing head list. The live
 * subscription and the history fetch both funnel through here so a
 * replacement that arrives while a fetch is in flight cannot be undone by the
 * slower of the two.
 */
export function mergeHeads(
  existing: readonly RelayEvent[],
  incoming: readonly RelayEvent[],
): RelayEvent[] {
  return resolveHeadList([...existing, ...incoming]);
}
