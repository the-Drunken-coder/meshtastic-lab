import { expect, test } from "@playwright/test";
import lineScenario from "../../scenarios/five-node-line.json" with { type: "json" };

test("five-node line lifecycle and traffic", async ({ page, request }) => {
  await request.post("/api/simulation/stop");
  const replaced = await request.put("/api/scenario", { data: lineScenario });
  expect(replaced.ok()).toBeTruthy();

  await page.goto("/");
  await expect(page.getByText("five-node-line", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Start", exact: true }).click();
  await expect(page.getByText("RUNNING", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: "Disable link from Node 2 to Node 3" }).click();
  await expect(page.getByText(/node-2 to node-3 disabled/)).toBeVisible();

  await page.getByLabel("Duration seconds").fill("6");
  await page.getByLabel("Messages/min/source").fill("30");
  await page.getByRole("button", { name: "Start traffic run" }).click();
  await expect(page.getByText(/Traffic run .* started/)).toBeVisible();
  await expect(page.locator(".metric").filter({ hasText: "Generated" }).locator("strong")).not.toHaveText("0");

  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("STOPPED", { exact: true }).first()).toBeVisible();
});
