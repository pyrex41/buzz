import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  coordinateOf,
  formatCoordinate,
  mergeHeads,
  parseCoordinate,
  resolveHeadList,
  resolveHeads,
  supersedes,
  tagValue,
  tagValues,
} from "./heads.ts";

/** Minimal RelayEvent shaped just enough for the reduce under test. */
function event({
  id,
  pubkey = "aa",
  kind = 35000,
  d = "ws-1",
  createdAt,
  tags = [],
}) {
  return {
    id,
    pubkey,
    kind,
    created_at: createdAt,
    content: "",
    sig: "",
    tags: [["d", d], ...tags],
  };
}

describe("parseCoordinate", () => {
  it("parses kind:pubkey:d", () => {
    assert.deepEqual(parseCoordinate("35000:abc:thermal-v2"), {
      kind: 35000,
      pubkey: "abc",
      id: "thermal-v2",
    });
  });

  it("round-trips through formatCoordinate", () => {
    const raw = "35001:deadbeef:calibrate-probe";
    assert.equal(formatCoordinate(parseCoordinate(raw)), raw);
  });

  it("splits on the first two colons so a colon-bearing d survives", () => {
    assert.deepEqual(parseCoordinate("35002:abc:weird:id"), {
      kind: 35002,
      pubkey: "abc",
      id: "weird:id",
    });
  });

  it("rejects malformed values rather than throwing", () => {
    for (const bad of [
      "",
      ":",
      "35000",
      "35000:abc",
      "notakind:abc:d",
      "35000::d",
      "35000:abc:",
    ]) {
      assert.equal(
        parseCoordinate(bad),
        null,
        `expected null for ${JSON.stringify(bad)}`,
      );
    }
  });
});

describe("tagValue / tagValues", () => {
  const e = event({
    id: "1",
    createdAt: 10,
    tags: [
      ["h", "chan-a"],
      ["h", "chan-b"],
      ["name", "Thermal"],
    ],
  });

  it("returns the first value of a tag", () => {
    assert.equal(tagValue(e, "h"), "chan-a");
    assert.equal(tagValue(e, "name"), "Thermal");
  });

  it("returns undefined for an absent tag", () => {
    assert.equal(tagValue(e, "status"), undefined);
  });

  it("collects every value in event order", () => {
    assert.deepEqual(tagValues(e, "h"), ["chan-a", "chan-b"]);
    assert.deepEqual(tagValues(e, "missing"), []);
  });
});

describe("coordinateOf", () => {
  it("derives the coordinate from kind, pubkey, and d", () => {
    assert.deepEqual(
      coordinateOf(
        event({
          id: "1",
          pubkey: "ff",
          kind: 35001,
          d: "task-9",
          createdAt: 1,
        }),
      ),
      { kind: 35001, pubkey: "ff", id: "task-9" },
    );
  });

  it("returns null when the d tag is missing or empty", () => {
    const noD = {
      id: "1",
      pubkey: "ff",
      kind: 35000,
      created_at: 1,
      content: "",
      sig: "",
      tags: [],
    };
    assert.equal(coordinateOf(noD), null);
    assert.equal(coordinateOf({ ...noD, tags: [["d", ""]] }), null);
  });
});

describe("supersedes", () => {
  it("prefers the higher created_at", () => {
    const older = event({ id: "bb", createdAt: 10 });
    const newer = event({ id: "aa", createdAt: 20 });
    assert.equal(supersedes(newer, older), true);
    assert.equal(supersedes(older, newer), false);
  });

  it("breaks created_at ties on the smaller event id", () => {
    const small = event({ id: "aaa", createdAt: 10 });
    const large = event({ id: "bbb", createdAt: 10 });
    assert.equal(supersedes(small, large), true);
    assert.equal(supersedes(large, small), false);
  });
});

describe("resolveHeads (LWW coordinate reduce)", () => {
  it("keeps the newest event per coordinate", () => {
    const heads = resolveHeads([
      event({ id: "old", d: "ws-1", createdAt: 100 }),
      event({ id: "new", d: "ws-1", createdAt: 200 }),
    ]);
    assert.equal(heads.size, 1);
    assert.equal(heads.get("35000:aa:ws-1").id, "new");
  });

  it("is order-independent — the same set yields the same winner", () => {
    const a = event({ id: "old", d: "ws-1", createdAt: 100 });
    const b = event({ id: "new", d: "ws-1", createdAt: 200 });
    assert.equal(resolveHeads([a, b]).get("35000:aa:ws-1").id, "new");
    assert.equal(resolveHeads([b, a]).get("35000:aa:ws-1").id, "new");
  });

  it("tie-breaks equal created_at on the smallest id, either order", () => {
    const a = event({ id: "aaa", d: "ws-1", createdAt: 100 });
    const z = event({ id: "zzz", d: "ws-1", createdAt: 100 });
    assert.equal(resolveHeads([a, z]).get("35000:aa:ws-1").id, "aaa");
    assert.equal(resolveHeads([z, a]).get("35000:aa:ws-1").id, "aaa");
  });

  it("separates coordinates by d tag", () => {
    const heads = resolveHeads([
      event({ id: "1", d: "ws-1", createdAt: 100 }),
      event({ id: "2", d: "ws-2", createdAt: 50 }),
    ]);
    assert.deepEqual([...heads.keys()].sort(), [
      "35000:aa:ws-1",
      "35000:aa:ws-2",
    ]);
  });

  it("separates coordinates by author — two people may use the same d", () => {
    const heads = resolveHeads([
      event({ id: "1", pubkey: "aa", d: "ws-1", createdAt: 100 }),
      event({ id: "2", pubkey: "bb", d: "ws-1", createdAt: 50 }),
    ]);
    assert.equal(heads.size, 2);
    assert.equal(heads.get("35000:bb:ws-1").id, "2");
  });

  it("separates coordinates by kind", () => {
    const heads = resolveHeads([
      event({ id: "1", kind: 35000, d: "x", createdAt: 100 }),
      event({ id: "2", kind: 35001, d: "x", createdAt: 100 }),
    ]);
    assert.equal(heads.size, 2);
  });

  it("drops events with no d tag — they address nothing", () => {
    const heads = resolveHeads([
      {
        id: "1",
        pubkey: "aa",
        kind: 35000,
        created_at: 1,
        content: "",
        sig: "",
        tags: [],
      },
    ]);
    assert.equal(heads.size, 0);
  });

  it("returns an empty map for no events", () => {
    assert.equal(resolveHeads([]).size, 0);
  });
});

describe("resolveHeadList", () => {
  it("sorts newest-first", () => {
    const list = resolveHeadList([
      event({ id: "1", d: "a", createdAt: 100 }),
      event({ id: "2", d: "b", createdAt: 300 }),
      event({ id: "3", d: "c", createdAt: 200 }),
    ]);
    assert.deepEqual(
      list.map((e) => e.id),
      ["2", "3", "1"],
    );
  });

  it("orders equal timestamps deterministically by id", () => {
    const list = resolveHeadList([
      event({ id: "zzz", d: "a", createdAt: 100 }),
      event({ id: "aaa", d: "b", createdAt: 100 }),
    ]);
    assert.deepEqual(
      list.map((e) => e.id),
      ["aaa", "zzz"],
    );
  });
});

describe("mergeHeads", () => {
  it("lets a newer incoming event replace the held head", () => {
    const held = [event({ id: "old", d: "ws-1", createdAt: 100 })];
    const merged = mergeHeads(held, [
      event({ id: "new", d: "ws-1", createdAt: 200 }),
    ]);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].id, "new");
  });

  it("ignores a stale incoming event — a late fetch cannot undo a live replace", () => {
    const held = [event({ id: "new", d: "ws-1", createdAt: 200 })];
    const merged = mergeHeads(held, [
      event({ id: "old", d: "ws-1", createdAt: 100 }),
    ]);
    assert.equal(merged[0].id, "new");
  });

  it("appends coordinates it has not seen", () => {
    const merged = mergeHeads(
      [event({ id: "1", d: "ws-1", createdAt: 100 })],
      [event({ id: "2", d: "ws-2", createdAt: 200 })],
    );
    assert.equal(merged.length, 2);
  });
});
