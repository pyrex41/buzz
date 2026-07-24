import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  DECISION_STATUSES,
  DECISION_STATUS_LABELS,
  REVIEW_VERDICTS,
  REVIEW_VERDICT_LABELS,
  TASK_STATUSES,
  TASK_STATUS_LABELS,
  WORKSTREAM_STATUSES,
  WORKSTREAM_STATUS_LABELS,
  WS_TYPES,
  WS_TYPE_LABELS,
  entityIdFromName,
  isTaskStatus,
  isValidEntityId,
  isWsType,
  parseDecisionStatus,
  parseReviewVerdict,
  parseTaskStatus,
  parseWorkstreamStatus,
  parseWsType,
} from "./vocab.ts";

describe("vocabularies match the relay contract (Hive §5.1–5.2)", () => {
  it("ws-type is the eight §5.2 values", () => {
    assert.deepEqual(
      [...WS_TYPES],
      [
        "code",
        "systems",
        "hardware",
        "data",
        "design",
        "process",
        "docs",
        "general",
      ],
    );
  });

  it("workstream status vocabulary", () => {
    assert.deepEqual(
      [...WORKSTREAM_STATUSES],
      ["active", "paused", "done", "archived"],
    );
  });

  it("task status vocabulary, in board-column order", () => {
    assert.deepEqual(
      [...TASK_STATUSES],
      ["todo", "in-progress", "blocked", "in-review", "done", "cancelled"],
    );
  });

  it("decision status vocabulary", () => {
    assert.deepEqual(
      [...DECISION_STATUSES],
      ["proposed", "accepted", "rejected", "superseded"],
    );
  });

  it("review verdict vocabulary", () => {
    assert.deepEqual(
      [...REVIEW_VERDICTS],
      ["approve", "request-changes", "reject"],
    );
  });

  it("every vocabulary member has a label", () => {
    for (const v of WS_TYPES) assert.ok(WS_TYPE_LABELS[v], `ws-type ${v}`);
    for (const v of WORKSTREAM_STATUSES)
      assert.ok(WORKSTREAM_STATUS_LABELS[v], v);
    for (const v of TASK_STATUSES) assert.ok(TASK_STATUS_LABELS[v], v);
    for (const v of DECISION_STATUSES) assert.ok(DECISION_STATUS_LABELS[v], v);
    for (const v of REVIEW_VERDICTS) assert.ok(REVIEW_VERDICT_LABELS[v], v);
  });
});

describe("parsers are total", () => {
  it("passes through known values", () => {
    assert.equal(parseWsType("hardware"), "hardware");
    assert.equal(parseWorkstreamStatus("paused"), "paused");
    assert.equal(parseTaskStatus("in-review"), "in-review");
    assert.equal(parseDecisionStatus("superseded"), "superseded");
    assert.equal(parseReviewVerdict("approve"), "approve");
  });

  it("falls back rather than dropping the row for an unknown value", () => {
    assert.equal(parseWsType("firmware"), "general");
    assert.equal(parseWorkstreamStatus("frozen"), "active");
    assert.equal(parseTaskStatus("wontfix"), "todo");
    assert.equal(parseDecisionStatus("maybe"), "proposed");
    assert.equal(parseReviewVerdict("shrug"), "request-changes");
  });

  it("falls back for undefined (tag absent)", () => {
    assert.equal(parseWsType(undefined), "general");
    assert.equal(parseTaskStatus(undefined), "todo");
  });

  it("does not accept a differently-cased value", () => {
    assert.equal(parseTaskStatus("In-Progress"), "todo");
  });
});

describe("type guards", () => {
  it("isWsType / isTaskStatus accept only exact members", () => {
    assert.equal(isWsType("design"), true);
    assert.equal(isWsType("Design"), false);
    assert.equal(isTaskStatus("blocked"), true);
    assert.equal(isTaskStatus("blocking"), false);
  });
});

describe("entity ids", () => {
  it("accepts the SDK's [a-zA-Z0-9._-]{1,64}", () => {
    assert.equal(isValidEntityId("thermal-v2"), true);
    assert.equal(isValidEntityId("adr_0002.final"), true);
    assert.equal(isValidEntityId("a".repeat(64)), true);
  });

  it("rejects empty, over-long, and colon-bearing ids", () => {
    assert.equal(isValidEntityId(""), false);
    assert.equal(isValidEntityId("a".repeat(65)), false);
    // A colon would make the <kind>:<pubkey>:<d> coordinate ambiguous.
    assert.equal(isValidEntityId("ws:1"), false);
    assert.equal(isValidEntityId("has space"), false);
    assert.equal(isValidEntityId("emoji-🔥"), false);
  });

  it("derives a valid id from a human name", () => {
    assert.equal(isValidEntityId(entityIdFromName("Thermal chamber v2")), true);
    assert.ok(
      entityIdFromName("Thermal chamber v2").startsWith("thermal-chamber-v2-"),
    );
  });

  it("derives a valid id even when the name has nothing usable", () => {
    assert.equal(isValidEntityId(entityIdFromName("🔥🔥🔥")), true);
    assert.equal(isValidEntityId(entityIdFromName("")), true);
  });

  it("keeps derived ids inside the 64-char budget for a long name", () => {
    const id = entityIdFromName("x".repeat(200));
    assert.equal(isValidEntityId(id), true);
  });

  it("does not collide for the same name across calls", () => {
    assert.notEqual(
      entityIdFromName("Same name"),
      entityIdFromName("Same name"),
    );
  });
});
