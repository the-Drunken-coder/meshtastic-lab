import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "tests",
  timeout: 180_000,
  expect: { timeout: 120_000 },
  workers: 1,
  use: {
    baseURL: process.env.MESHTASTIC_LAB_URL ?? "http://127.0.0.1:8080",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
});
