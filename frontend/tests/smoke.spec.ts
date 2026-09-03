import { expect, test } from "@playwright/test";
import lineScenario from "../../scenarios/five-node-line.json" with { type: "json" };

test("five-node line lifecycle and traffic", async ({ page, request }) => {
  await request.post("/api/simulation/stop");
  const replaced = await request.put("/api/scenario", { data: lineScenario });
  expect(replaced.ok()).toBeTruthy();

  let initialCapabilitiesFailed = false;
  await page.route("**/api/capabilities", async (route) => {
    if (!initialCapabilitiesFailed) {
      initialCapabilitiesFailed = true;
      await route.fulfill({ status: 503, json: { error: { message: "temporary failure" } } });
      return;
    }
    await route.continue();
  });

  await page.goto("/");
  await expect(page.getByRole("alert")).toContainText("HTTP_503: temporary failure");
  await expect(page.getByText("five-node-line", { exact: true })).toBeVisible();

  const externalScenario = {
    ...lineScenario,
    name: "five-node-line-external",
    nodes: lineScenario.nodes.map((node) =>
      node.id === "node-1" ? { ...node, displayName: "External Node 1" } : node,
    ),
    links: lineScenario.links.map((link) =>
      link.from === "node-1" && link.to === "node-2"
        ? { ...link, rssiDbm: -101, snrDb: 2.5 }
        : link,
    ),
  };
  const externalUpdate = await request.put("/api/scenario", { data: externalScenario });
  expect(externalUpdate.ok()).toBeTruthy();
  await expect(page.getByText("five-node-line-external", { exact: true })).toBeVisible();
  await expect(page.getByText("External Node 1", { exact: true }).first()).toBeVisible();

  await page.getByLabel("Nodes").fill("6");
  await expect(
    page.getByRole("button", { name: "Disable link from External Node 1 to Node 6" }),
  ).toBeVisible();
  await page.getByLabel("Nodes").fill("5");
  await page.getByRole("button", { name: "Save scenario" }).click();
  const resizedScenarioResponse = await request.get("/api/scenario");
  const resizedScenario = (await resizedScenarioResponse.json()) as typeof lineScenario;
  expect(
    resizedScenario.links.find(
      (link) => link.from === "node-1" && link.to === "node-2",
    ),
  ).toMatchObject({ enabled: true, rssiDbm: -101, snrDb: 2.5 });
  expect(
    resizedScenario.links.find(
      (link) => link.from === "node-1" && link.to === "node-3",
    ),
  ).toMatchObject({ enabled: false, rssiDbm: -85, snrDb: 8 });

  let pollFailurePending = true;
  await page.route(
    (url) => url.pathname === "/api/state",
    async (route) => {
      if (pollFailurePending) {
        pollFailurePending = false;
        await route.fulfill({ status: 503, json: { error: { message: "polling failed" } } });
        return;
      }
      await route.continue();
    },
  );
  await expect(page.getByRole("alert")).toContainText("HTTP_503: polling failed");

  await page.getByLabel("Frequency slot").fill("21");
  const runtimeScenario = {
    ...resizedScenario,
    name: "five-node-line-runtime",
    links: resizedScenario.links.map((link) =>
      link.from === "node-1" && link.to === "node-2"
        ? { ...link, rssiDbm: -111, snrDb: 1.5 }
        : link,
    ),
  };
  const runtimeUpdate = await request.put("/api/scenario", { data: runtimeScenario });
  expect(runtimeUpdate.ok()).toBeTruthy();
  const started = await request.post("/api/simulation/start");
  expect(started.ok()).toBeTruthy();
  await expect(page.getByText("RUNNING", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("five-node-line-runtime", { exact: true })).toBeVisible();

  await page
    .getByRole("button", { name: "Disable link from External Node 1 to Node 2" })
    .click();
  const runtimeLinkResponse = await request.get("/api/scenario");
  const runtimeLinkScenario = (await runtimeLinkResponse.json()) as typeof lineScenario;
  expect(
    runtimeLinkScenario.links.find(
      (link) => link.from === "node-1" && link.to === "node-2",
    ),
  ).toMatchObject({ enabled: false, rssiDbm: -111, snrDb: 1.5 });

  await page.route(
    (url) =>
      url.pathname === "/api/nodes/node-1/logs" && url.searchParams.get("stream") === "stderr",
    (route) =>
      route.fulfill({
        status: 200,
        json: { nodeId: "node-1", stream: "stderr", lines: ["polled log sentinel"] },
      }),
  );
  await expect(page.locator(".daemon-log")).toContainText("polled log sentinel");

  await page.getByLabel("Edit symmetrically").check();
  await page.getByRole("button", { name: "Disable link from Node 2 to Node 3" }).click();
  await expect(page.getByText(/node-2 to node-3 disabled/)).toBeVisible();
  const changedScenarioResponse = await request.get("/api/scenario");
  const changedScenario = (await changedScenarioResponse.json()) as typeof lineScenario;
  expect(
    changedScenario.links
      .filter(
        (link) =>
          (link.from === "node-2" && link.to === "node-3") ||
          (link.from === "node-3" && link.to === "node-2"),
      )
      .every((link) => !link.enabled),
  ).toBeTruthy();

  await page.getByLabel("Duration seconds").fill("6");
  await page.getByLabel("Messages/min/source").fill("30");
  await page.getByRole("button", { name: "Start traffic run" }).click();
  await expect(page.getByText(/Traffic run .* started/)).toBeVisible();
  await expect(page.locator(".metric").filter({ hasText: "Generated" }).locator("strong")).not.toHaveText("0");

  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("STOPPED", { exact: true }).first()).toBeVisible();
  await expect(page.locator(".daemon-log")).toContainText("polled log sentinel");
});
