import { type Page, expect, test } from "@playwright/test";

import { waitForAnimations } from "../helpers/animations";
import { installMockBridge } from "../helpers/bridge";

// The mock relay's starter `#general` channel. Every seeded event is `h`-tagged
// to it, and `openWorkstreams` selects it explicitly.
const GENERAL_CHANNEL_ID = "9a1657ac-f7aa-5db0-b632-d8bbeb6dfb50";

const AUTHOR = "deadbeef".repeat(8);
const WORKSTREAM_ID = "thermal-v2";
const WORKSTREAM_ADDRESS = `35000:${AUTHOR}:${WORKSTREAM_ID}`;

type SeedEvent = {
  id: string;
  pubkey: string;
  created_at: number;
  kind: number;
  tags: string[][];
  content: string;
  sig: string;
};

function seedEvent(
  kind: number,
  id: string,
  tags: string[][],
  content = "",
  createdAt = 1_700_000_000,
): SeedEvent {
  return {
    id: id.padEnd(64, "0"),
    pubkey: AUTHOR,
    created_at: createdAt,
    kind,
    tags: [...tags, ["h", GENERAL_CHANNEL_ID]],
    content,
    sig: "0".repeat(128),
  };
}

function task(
  id: string,
  name: string,
  status: string,
  extra: string[][] = [],
): SeedEvent {
  return seedEvent(35001, `task${id}`, [
    ["d", id],
    ["a", WORKSTREAM_ADDRESS],
    ["status", status],
    ["name", name],
    ...extra,
  ]);
}

/** The fixture: one hardware workstream with tasks spread across the board. */
const SEED_EVENTS: SeedEvent[] = [
  seedEvent(
    35000,
    "ws1",
    [
      ["d", WORKSTREAM_ID],
      ["ws-type", "hardware"],
      ["status", "active"],
      ["name", "Thermal chamber v2"],
    ],
    "Bring-up of the second thermal chamber.",
  ),
  task("calibrate-probe", "Calibrate the thermocouple probe", "todo", [
    ["due", "2026-08-01"],
  ]),
  task("order-pt100", "Order PT100 probes", "in-progress"),
  task("chamber-seal", "Chase the chamber door seal", "blocked"),
  task("bom-review", "Review the bill of materials", "in-review"),
  task("bench-setup", "Set up the bench", "done"),
  // An event replaced under NIP-33: same (kind, pubkey, d) as `bench-setup`
  // but newer, so the LWW reduce must show only the "cancelled" revision.
  {
    ...task("bench-setup", "Set up the bench", "cancelled"),
    id: "taskbench-setup-v2".padEnd(64, "0"),
    created_at: 1_700_000_500,
  },
];

/**
 * Load the mock relay's Workstream store.
 *
 * The bridge installs its seed hooks during bootstrap, which can finish after
 * `page.goto` resolves — so wait for the hook rather than calling it
 * optionally. An `?.()` against a not-yet-installed hook silently does
 * nothing, which shows up later as an intermittently empty list rather than
 * as the setup failure it actually is.
 */
async function seedWorkstreams(page: Page, events: SeedEvent[]) {
  await page.waitForFunction(
    () => typeof window.__BUZZ_E2E_SEED_MOCK_WORKSTREAMS__ === "function",
  );
  await page.evaluate((seeded) => {
    const seed = window.__BUZZ_E2E_SEED_MOCK_WORKSTREAMS__;
    if (seed === undefined) {
      throw new Error("mock bridge did not install the workstream seed hook");
    }
    seed(
      seeded as unknown as Parameters<
        NonNullable<typeof window.__BUZZ_E2E_SEED_MOCK_WORKSTREAMS__>
      >[0],
    );
  }, events);
}

/**
 * Open the view and pin it to `#general`. The channel is chosen explicitly
 * rather than relying on the default: which channel sorts first is a property
 * of the sidebar's ordering, not of this feature, and letting it decide would
 * make these tests fail on an unrelated sort change.
 */
async function openWorkstreams(page: Page) {
  await page.getByTestId("open-workstreams-view").click();
  await expect(page).toHaveURL(/#\/workstreams$/);
  await expect(page.getByTestId("workstreams-view")).toBeVisible();

  await page.getByTestId("workstreams-channel-picker").click();
  await waitForAnimations(page);
  await page.getByTestId("workstreams-channel-general").click();
  await expect(page.getByTestId("workstreams-channel-picker")).toContainText(
    "#general",
  );
}

test.beforeEach(async ({ page }) => {
  await installMockBridge(page);
});

test("sidebar entry opens an empty workstreams list", async ({ page }) => {
  await page.goto("/");
  await openWorkstreams(page);

  await expect(page.getByTestId("workstreams-empty")).toBeVisible();
});

test("list renders seeded workstream heads with type and status chips", async ({
  page,
}) => {
  await page.goto("/");
  await seedWorkstreams(page, SEED_EVENTS);
  await openWorkstreams(page);

  const row = page.getByTestId(`workstream-row-${WORKSTREAM_ID}`);
  await expect(row).toBeVisible();
  await expect(row).toContainText("Thermal chamber v2");
  await expect(row.getByTestId("workstream-type-hardware")).toBeVisible();
  await expect(row.getByTestId("workstream-status-active")).toBeVisible();
});

test("create dialog publishes a workstream that appears in the list", async ({
  page,
}) => {
  await page.goto("/");
  await openWorkstreams(page);

  await page.getByTestId("create-workstream-open").click();
  const dialog = page.getByTestId("create-workstream-dialog");
  await expect(dialog).toBeVisible();
  await waitForAnimations(page);

  await dialog.getByTestId("create-workstream-name").fill("Chamber bring-up");
  await dialog.getByTestId("create-workstream-type-hardware").click();
  await dialog
    .getByTestId("create-workstream-description")
    .fill("Second thermal chamber.");
  await dialog.getByTestId("create-workstream-submit").click();

  await expect(dialog).toBeHidden();

  const list = page.getByTestId("workstreams-list");
  await expect(list).toBeVisible();
  await expect(list).toContainText("Chamber bring-up");
  await expect(list.getByTestId("workstream-type-hardware")).toBeVisible();

  // The published event carries the §5.1 tag shape the relay validates.
  const signed = await page.evaluate(() =>
    (window.__BUZZ_E2E_SIGNED_EVENTS__ ?? []).filter(
      (event) => event.kind === 35000,
    ),
  );
  expect(signed).toHaveLength(1);
  const tagNames = signed[0].tags.map((tag) => tag[0]);
  expect(tagNames).toEqual(
    expect.arrayContaining(["d", "ws-type", "status", "name", "h"]),
  );
  expect(signed[0].tags).toContainEqual(["ws-type", "hardware"]);
  expect(signed[0].tags).toContainEqual(["h", GENERAL_CHANNEL_ID]);
});

test("task board renders a column per status with LWW-resolved cards", async ({
  page,
}) => {
  await page.goto("/");
  await seedWorkstreams(page, SEED_EVENTS);
  await openWorkstreams(page);

  await page.getByTestId(`workstream-row-${WORKSTREAM_ID}`).click();
  await expect(page).toHaveURL(new RegExp(`#/workstreams/${WORKSTREAM_ID}$`));

  await expect(page.getByTestId("workstream-detail-name")).toHaveText(
    "Thermal chamber v2",
  );

  const board = page.getByTestId("task-board");
  await expect(board).toBeVisible();

  // Every status gets a column, including the empty ones.
  for (const status of [
    "todo",
    "in-progress",
    "blocked",
    "in-review",
    "done",
    "cancelled",
  ]) {
    await expect(page.getByTestId(`task-column-${status}`)).toBeVisible();
  }

  await expect(page.getByTestId("task-card-calibrate-probe")).toContainText(
    "Calibrate the thermocouple probe",
  );
  await expect(page.getByTestId("task-due-calibrate-probe")).toContainText(
    "2026-08-01",
  );

  // `bench-setup` was published twice; only the newer revision counts, so it
  // sits in Cancelled and Done is empty.
  await expect(
    page
      .getByTestId("task-column-cancelled")
      .getByTestId("task-card-bench-setup"),
  ).toBeVisible();
  await expect(page.getByTestId("task-column-count-done")).toHaveText("0");
  await expect(page.getByTestId("task-column-count-cancelled")).toHaveText("1");
});

test("moving a task emits a status change and replaces the head", async ({
  page,
}) => {
  await page.goto("/");
  await seedWorkstreams(page, SEED_EVENTS);
  await openWorkstreams(page);

  await page.getByTestId(`workstream-row-${WORKSTREAM_ID}`).click();
  await expect(page.getByTestId("task-board")).toBeVisible();

  await page.getByTestId("task-move-calibrate-probe").click();
  await waitForAnimations(page);
  await page.getByTestId("task-move-calibrate-probe-in-progress").click();

  await expect(
    page
      .getByTestId("task-column-in-progress")
      .getByTestId("task-card-calibrate-probe"),
  ).toBeVisible();

  const signed = await page.evaluate(
    () => window.__BUZZ_E2E_SIGNED_EVENTS__ ?? [],
  );

  // 47001 records the transition; the 35001 head is replaced to match.
  const statusChange = signed.find((event) => event.kind === 47001);
  expect(statusChange).toBeDefined();
  expect(statusChange?.tags).toContainEqual(["status", "in-progress"]);
  expect(statusChange?.tags).toContainEqual(["previous-status", "todo"]);
  expect(statusChange?.tags).toContainEqual([
    "a",
    `35001:${AUTHOR}:calibrate-probe`,
  ]);

  const head = signed.find((event) => event.kind === 35001);
  expect(head?.tags).toContainEqual(["status", "in-progress"]);
  expect(head?.tags).toContainEqual(["d", "calibrate-probe"]);
});

test("artifact and decision tabs render heads, versions, and review state", async ({
  page,
}) => {
  const artifactAddress = `35002:${AUTHOR}:chamber-bom`;
  const reviewRequestId = "req1".padEnd(64, "0");

  await page.goto("/");
  await seedWorkstreams(page, [
    ...SEED_EVENTS,
    seedEvent(35002, "art1", [
      ["d", "chamber-bom"],
      ["artifact-type", "bom"],
      ["name", "Thermal chamber bill of materials"],
      ["a", WORKSTREAM_ADDRESS],
      ["version", "v2"],
    ]),
    seedEvent(
      47002,
      "ver1",
      [
        ["a", artifactAddress],
        ["version", "v2"],
        ["content-hash", "f".repeat(64)],
      ],
      "swapped in PT100 probes",
    ),
    seedEvent(47010, "req1", [["a", artifactAddress]], "Please sanity-check."),
    seedEvent(
      47012,
      "dec1",
      [
        ["a", artifactAddress],
        ["e", reviewRequestId, "", "reply"],
        ["decision", "approve"],
      ],
      "Looks right.",
    ),
    seedEvent(
      35003,
      "adr1",
      [
        ["d", "adr-0001"],
        ["a", WORKSTREAM_ADDRESS],
        ["status", "superseded"],
        ["name", "Use thermocouples"],
      ],
      "",
    ),
    seedEvent(
      35003,
      "adr2",
      [
        ["d", "adr-0002"],
        ["a", WORKSTREAM_ADDRESS],
        ["status", "accepted"],
        ["name", "Use PT100 probes"],
        ["supersedes", `35003:${AUTHOR}:adr-0001`],
      ],
      "Thermocouples drift.",
    ),
  ]);
  await openWorkstreams(page);

  await page.getByTestId(`workstream-row-${WORKSTREAM_ID}`).click();
  await expect(page.getByTestId("task-board")).toBeVisible();

  await page.getByTestId("workstream-tab-artifacts").click();
  const artifact = page.getByTestId("artifact-card-chamber-bom");
  await expect(artifact).toBeVisible();
  await expect(artifact).toContainText("Thermal chamber bill of materials");
  await expect(artifact.getByTestId("artifact-version-chamber-bom")).toHaveText(
    "v2",
  );
  await expect(artifact).toContainText("swapped in PT100 probes");
  await expect(artifact.getByTestId("review-verdict-approve")).toBeVisible();

  await page.getByTestId("workstream-tab-decisions").click();
  // Only the chain head gets a row; its predecessor shows as supersession.
  const decision = page.getByTestId("decision-card-adr-0002");
  await expect(decision).toBeVisible();
  await expect(decision.getByTestId("decision-status-accepted")).toBeVisible();
  await expect(page.getByTestId("decision-card-adr-0001")).toHaveCount(0);
  await expect(page.getByTestId("decision-chain-adr-0002")).toContainText(
    "Use thermocouples",
  );
});
