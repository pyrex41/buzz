import assert from "node:assert/strict";
import test from "node:test";

import {
  deriveShellRoute,
  shouldBounceForChannelNotification,
} from "./AppShell.helpers.ts";

test("shouldBounceForChannelNotification_allowsTopLevelChannelMessages", () => {
  assert.equal(shouldBounceForChannelNotification([["h", "channel"]]), true);
});

test("shouldBounceForChannelNotification_suppressesThreadReplies", () => {
  assert.equal(
    shouldBounceForChannelNotification([
      ["h", "channel"],
      ["e", "root", "", "reply"],
    ]),
    false,
  );
});

test("shouldBounceForChannelNotification_allowsBroadcastReplies", () => {
  assert.equal(
    shouldBounceForChannelNotification([
      ["h", "channel"],
      ["e", "root", "", "reply"],
      ["broadcast", "1"],
    ]),
    true,
  );
});

test("deriveShellRoute_selectsWorkstreamsForListAndDetail", () => {
  assert.deepEqual(deriveShellRoute("/workstreams"), {
    selectedChannelId: null,
    selectedView: "workstreams",
  });
  assert.deepEqual(deriveShellRoute("/workstreams/thermal-v2"), {
    selectedChannelId: null,
    selectedView: "workstreams",
  });
});

test("deriveShellRoute_keepsWorkflowsAndWorkstreamsDistinct", () => {
  // The two paths share a prefix up to "work"; a `startsWith` written against
  // the wrong stem would light up the wrong sidebar entry.
  assert.equal(deriveShellRoute("/workflows").selectedView, "workflows");
  assert.equal(deriveShellRoute("/workflows/abc").selectedView, "workflows");
  assert.equal(deriveShellRoute("/workstreams").selectedView, "workstreams");
});

test("deriveShellRoute_fallsBackToHomeForUnknownPaths", () => {
  assert.equal(deriveShellRoute("/nope").selectedView, "home");
  assert.equal(deriveShellRoute("/").selectedView, "home");
});
