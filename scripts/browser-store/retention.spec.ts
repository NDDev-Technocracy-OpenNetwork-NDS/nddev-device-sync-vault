import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { type BrowserContext, expect, test } from "@playwright/test";
import { startServer } from "./server";

test("retained OPFS survives old/new workers and concurrent reopen", async ({
  playwright,
  browserName,
}) => {
  const profile = await mkdtemp(join(tmpdir(), "nds-vault-browser-profile-"));
  let server: Awaited<ReturnType<typeof startServer>>["server"] | undefined;
  let context: BrowserContext | undefined;
  let profileReleased = true;
  let failure: unknown;
  try {
    ({ server } = await startServer(resolve(".nddev-reports/browser-store")));
    const origin = server.url.origin;
    profileReleased = false;
    context = await playwright[browserName].launchPersistentContext(profile, {
      headless: true,
    });
    const errors: string[] = [];
    context.on("page", (page) => page.on("pageerror", (error) => errors.push(error.message)));
    await context.route("**/*", (route) => {
      if (new URL(route.request().url()).origin === origin) return route.continue();
      return route.abort("blockedbyclient");
    });
    const page = await context.newPage();
    await page.goto(server.url.href);
    await expect(page.getByRole("status")).toHaveText("Passed", { timeout: 40_000 });
    expect(await page.evaluate(() => window.__journalFixture)).toEqual({
      status: "passed",
      phases: ["baseline", "candidate", "baseline", "candidate"],
      originals: 3,
      independentWorkers: 2,
    });
    expect(errors).toEqual([]);
  } catch (error) {
    failure = error;
  } finally {
    try {
      if (context) {
        await context.close();
        profileReleased = true;
      }
    } catch (error) {
      failure ??= error;
    }
    try {
      await server?.stop(true);
      // An uncertain browser lifetime preserves its own profile for inspection.
      if (profileReleased) await rm(profile, { recursive: true });
    } catch (error) {
      failure ??= error;
    }
  }
  if (failure !== undefined) throw failure;
});
