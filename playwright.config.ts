import { defineConfig } from "@playwright/test";

// Static fixture: no watch mode, custom loader or runner tsconfig override.
// Types are checked separately before the Bun-compatible test runner starts.
export default defineConfig({
  testDir: "./scripts/browser-store",
  testMatch: "**/*.spec.ts",
  fullyParallel: true,
  forbidOnly: true,
  retries: 0,
  workers: 3,
  timeout: 60_000,
  reporter: [["list"], ["junit", { outputFile: ".nddev-reports/browser-store-results.xml" }]],
  projects: [
    { name: "chromium", use: { browserName: "chromium" } },
    { name: "firefox", use: { browserName: "firefox" } },
    { name: "webkit", use: { browserName: "webkit" } },
  ],
});
