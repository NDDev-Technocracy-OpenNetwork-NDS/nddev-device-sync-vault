// browser_journal.js is generated from this TypeScript source. Do not edit its output.
// One owned replica worker per realm, bounded admission and response deadlines.
// Worker death is an unknown outcome, never a synthesized receipt.
import type { JournalInput, JournalRequest } from "./browser_messages";

let configuration: { worker: string; module: string } | undefined;
let active: { worker: Worker; scope: string } | undefined;
let sequence = 0;
let busy = false;
const keyLeases = new Map<string, { controller: AbortController; release: () => void }>();
type Waiter = { resolve: () => void; timer?: number };
const waiters: Waiter[] = [];

function admit(): Promise<void> {
  if (!busy) {
    busy = true;
    return Promise.resolve();
  }
  if (waiters.length >= 3) return Promise.reject(failed("vault_busy"));
  return new Promise<void>((resolve, reject) => {
    const entry: Waiter = {
      resolve: () => {
        clearTimeout(entry.timer);
        resolve();
      },
    };
    entry.timer = setTimeout(() => {
      const index = waiters.indexOf(entry);
      if (index >= 0) waiters.splice(index, 1);
      reject(failed("vault_busy"));
    }, 500);
    waiters.push(entry);
  });
}
function release(): void {
  const next = waiters.shift();
  if (next) next.resolve();
  else busy = false;
}
function failed(code = "vault_dependency_unavailable"): Error {
  return new Error(code);
}

export function configureJournal(worker: string, module: string): void {
  const workerUrl = new URL(worker, location.href);
  const moduleUrl = new URL(module, location.href);
  if (
    [workerUrl, moduleUrl].some(
      (url) =>
        url.origin !== location.origin || url.search || url.hash || !url.pathname.endsWith(".js"),
    )
  )
    throw failed();
  const next = { worker: workerUrl.href, module: moduleUrl.href };
  if (
    configuration &&
    (configuration.worker !== next.worker || configuration.module !== next.module)
  )
    throw failed();
  configuration = next;
}

function message(worker: Worker, data: JournalInput): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const id = ++sequence;
    const timer = setTimeout(() => {
      cleanup();
      worker.terminate();
      if (active?.worker === worker) active = undefined;
      reject(failed());
    }, 15000);
    const onMessage = ({ data }: MessageEvent<unknown>) => {
      if (typeof data !== "object" || data === null || !("id" in data) || data.id !== id) return;
      cleanup();
      if ("error" in data && data.error) {
        if (data.error !== "vault_busy") {
          worker.terminate();
          if (active?.worker === worker) active = undefined;
        }
        reject(failed(data.error === "vault_busy" ? "vault_busy" : undefined));
      } else if ("result" in data && typeof data.result === "string") resolve(data.result);
      else onError();
    };
    const onError = () => {
      cleanup();
      worker.terminate();
      if (active?.worker === worker) active = undefined;
      reject(failed());
    };
    function cleanup(): void {
      clearTimeout(timer);
      worker.removeEventListener("message", onMessage);
      worker.removeEventListener("error", onError);
    }
    worker.addEventListener("message", onMessage);
    worker.addEventListener("error", onError);
    worker.postMessage({ ...data, id } satisfies JournalRequest);
  });
}

export async function journalRequest(scope: string, command: string): Promise<string> {
  const configured = configuration;
  if (!configured || !/^[a-f0-9]{64}$/.test(scope) || command.length > 8 * 1024 * 1024)
    throw failed();
  // Foreground work and sync share this one queue, including the waiting budget.
  await admit();
  try {
    let owner = active;
    if (!owner || owner.scope !== scope) {
      owner?.worker.terminate();
      const worker = new Worker(configured.worker, { type: "module" });
      owner = { scope, worker };
      active = owner;
      try {
        await message(worker, { action: "initialize", scope, module: configured.module });
      } catch (error) {
        worker.terminate();
        active = undefined;
        throw error;
      }
    }
    return await message(owner.worker, { action: "execute", command });
  } finally {
    release();
  }
}

export function releaseKey(id: string): void {
  const lease = keyLeases.get(id);
  if (lease) {
    keyLeases.delete(id);
    lease.controller.abort();
    lease.release();
  }
}
export function acquireKey(id: string, scope: string): Promise<void> {
  if (
    !/^[a-f0-9-]{36}$/.test(id) ||
    !/^[a-f0-9]{64}$/.test(scope) ||
    keyLeases.has(id) ||
    keyLeases.size >= 4
  )
    return Promise.reject(failed());
  return new Promise<void>((resolve, reject) => {
    const controller = new AbortController();
    const ownership: { release?: () => void } = {};
    const held = new Promise<void>((done) => {
      ownership.release = done;
    });
    const release = ownership.release;
    if (!release) throw failed();
    const lease = { controller, release };
    keyLeases.set(id, lease);
    const timer = setTimeout(() => {
      controller.abort();
      reject(failed("vault_busy"));
    }, 500);
    navigator.locks
      .request(`nds-vault-key:${scope}`, { signal: controller.signal }, async (lock) => {
        if (!lock || !keyLeases.has(id)) throw failed();
        clearTimeout(timer);
        resolve();
        await held;
      })
      .catch(() => {
        if (keyLeases.get(id) === lease) keyLeases.delete(id);
        release();
        reject(failed("vault_busy"));
      });
    held.then(() => clearTimeout(timer));
  });
}

// Retention preference only: the user/browser can still remove site data.
export async function requestPersistence(): Promise<boolean> {
  try {
    if (!navigator.storage?.persisted || !navigator.storage?.persist) return false;
    if (await navigator.storage.persisted()) return true;
    await navigator.storage.persist();
    return await navigator.storage.persisted();
  } catch {
    return false;
  }
}
