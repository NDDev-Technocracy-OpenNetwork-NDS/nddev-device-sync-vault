// browser_worker.js is generated from this TypeScript source. Do not edit its output.
// The composition root exports exactly these journal entrypoints. The worker
// receives no credential material or arbitrary SQL.
import type { JournalResponse } from "./browser_messages";

async function loadModule(url: string) {
  const module: unknown = await import(url);
  if (
    typeof module !== "object" ||
    module === null ||
    !("default" in module) ||
    typeof module.default !== "function" ||
    !("nds_journal_open" in module) ||
    typeof module.nds_journal_open !== "function" ||
    !("nds_journal_resume" in module) ||
    typeof module.nds_journal_resume !== "function" ||
    !("nds_journal_execute" in module) ||
    typeof module.nds_journal_execute !== "function" ||
    !("nds_journal_pause" in module) ||
    typeof module.nds_journal_pause !== "function"
  )
    throw new Error();
  const {
    nds_journal_open: open,
    nds_journal_resume: resume,
    nds_journal_execute: execute,
    nds_journal_pause: pause,
  } = module;
  await module.default();
  return {
    async open(scope: string): Promise<void> {
      await open(scope);
    },
    async resume(): Promise<void> {
      await resume();
    },
    execute(command: string): string {
      const result: unknown = execute(command);
      if (typeof result !== "string") throw new Error();
      return result;
    },
    pause(): void {
      pause();
    },
  };
}

let active: { scope: string; module: Awaited<ReturnType<typeof loadModule>> } | undefined;
let busy = false;
const respond = (reply: JournalResponse): void => self.postMessage(reply);
self.onmessage = async ({ data }: MessageEvent<unknown>) => {
  if (typeof data !== "object" || data === null || !("id" in data) || typeof data.id !== "number")
    throw new Error();
  const id = data.id;
  if (busy) {
    respond({ id, error: "vault_busy" });
    return;
  }
  busy = true;
  const controller = new AbortController();
  let timer: number | undefined;
  async function locked<T>(scope: string, operation: () => Promise<T>): Promise<T> {
    timer = setTimeout(() => controller.abort(), 1000);
    return navigator.locks.request(
      `nds-replica:${scope}`,
      { signal: controller.signal },
      async () => {
        clearTimeout(timer);
        return operation();
      },
    );
  }
  try {
    if (
      "action" in data &&
      data.action === "initialize" &&
      "module" in data &&
      typeof data.module === "string" &&
      "scope" in data &&
      typeof data.scope === "string"
    ) {
      const url = new URL(data.module);
      if (
        active ||
        url.origin !== location.origin ||
        !url.pathname.endsWith(".js") ||
        url.search ||
        url.hash ||
        !/^[a-f0-9]{64}$/.test(data.scope)
      )
        throw new Error();
      const module = await loadModule(url.href);
      const scope = data.scope;
      active = { module, scope };
      await locked(scope, () => module.open(scope));
      respond({ id, result: "ready" });
    } else if (
      "action" in data &&
      data.action === "execute" &&
      active &&
      "command" in data &&
      typeof data.command === "string" &&
      data.command.length <= 8 * 1024 * 1024
    ) {
      const { module, scope } = active;
      const command = data.command;
      const result = await locked(scope, async () => {
        await module.resume();
        try {
          return module.execute(command);
        } finally {
          module.pause();
        }
      });
      respond({ id, result });
    } else throw new Error();
  } catch {
    respond({
      id,
      error: controller.signal.aborted ? "vault_busy" : "vault_dependency_unavailable",
    });
    // A trap or failed pause can retain SQLite; terminating this owned realm
    // releases OPFS handles before another caller attempts recovery.
    if (!controller.signal.aborted) self.close();
  } finally {
    clearTimeout(timer);
    busy = false;
  }
};
