// Unit tests for the browser journal client. The OPFS worker itself stays
// covered by owned backend acceptance; these tests exercise the client's
// bounded admission, lifecycle and lease guards with a stubbed worker.
import { beforeAll, describe, expect, test } from "bun:test";
import {
  acquireKey,
  configureJournal,
  journalRequest,
  releaseKey,
  requestPersistence,
} from "./browser_journal";

const ORIGIN = "https://vault.example";
const SCOPE = "a".repeat(64);
const OTHER_SCOPE = "b".repeat(64);

// A compliant worker double: answers every posted request with its own id.
class FakeWorker {
  static instances: FakeWorker[] = [];
  terminated = false;
  posted: unknown[] = [];
  private listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(
    public url: string,
    public options: { type: string },
  ) {
    FakeWorker.instances.push(this);
  }
  addEventListener(kind: string, listener: (event: unknown) => void) {
    this.listeners.set(kind, (this.listeners.get(kind) ?? new Set()).add(listener));
  }
  removeEventListener(kind: string, listener: (event: unknown) => void) {
    this.listeners.get(kind)?.delete(listener);
  }
  postMessage(data: unknown) {
    this.posted.push(data);
    const id = (data as { id: number }).id;
    setTimeout(() => {
      for (const listener of this.listeners.get("message") ?? []) {
        listener({ data: { id, result: "receipt" } });
      }
    }, 0);
  }
  terminate() {
    this.terminated = true;
  }
}

function installLocks(grant: { name: string } | null = { name: "lock" }) {
  Object.defineProperty(globalThis, "navigator", {
    value: {
      locks: {
        request: (
          _name: string,
          _options: { signal: AbortSignal },
          run: (lock: { name: string } | null) => unknown,
        ) => run(grant),
      },
    },
    configurable: true,
  });
}

beforeAll(() => {
  Object.defineProperty(globalThis, "location", {
    value: { href: `${ORIGIN}/app/index.html`, origin: ORIGIN },
    configurable: true,
  });
  Object.defineProperty(globalThis, "Worker", { value: FakeWorker, configurable: true });
  configureJournal("./journal_worker.js", "./journal_module.js");
});

describe("configureJournal", () => {
  test("accepts an identical reconfiguration and rejects a different one", () => {
    configureJournal("./journal_worker.js", "./journal_module.js");
    expect(() => configureJournal("./other.js", "./journal_module.js")).toThrow(
      "vault_dependency_unavailable",
    );
  });

  test("rejects foreign origins, queries, fragments and non-js paths", () => {
    expect(() => configureJournal("https://evil.example/w.js", "./m.js")).toThrow(
      "vault_dependency_unavailable",
    );
    expect(() => configureJournal("./w.js?x=1", "./m.js")).toThrow("vault_dependency_unavailable");
    expect(() => configureJournal("./w.js#f", "./m.js")).toThrow("vault_dependency_unavailable");
    expect(() => configureJournal("./w.txt", "./m.js")).toThrow("vault_dependency_unavailable");
  });
});

describe("journalRequest guards", () => {
  test("rejects malformed scopes and oversized commands", async () => {
    await expect(journalRequest("not-hex", "op")).rejects.toThrow("vault_dependency_unavailable");
    await expect(journalRequest(SCOPE, "x".repeat(8 * 1024 * 1024 + 1))).rejects.toThrow(
      "vault_dependency_unavailable",
    );
  });
});

describe("journalRequest lifecycle", () => {
  test("initializes the worker once and returns the response result", async () => {
    const count = FakeWorker.instances.length;
    const first = await journalRequest(SCOPE, "list");
    const second = await journalRequest(SCOPE, "status");
    expect(first).toBe("receipt");
    expect(second).toBe("receipt");
    expect(FakeWorker.instances.length).toBe(count + 1);
  });

  test("recreates the worker when the scope changes", async () => {
    const previous = FakeWorker.instances.at(-1);
    await journalRequest(OTHER_SCOPE, "list");
    expect(previous?.terminated).toBe(true);
  });
});

describe("key leases", () => {
  test("admits a bounded number of keys and validates identifiers", async () => {
    installLocks();
    const id =
      "0".repeat(8) +
      "-" +
      "0".repeat(4) +
      "-" +
      "0".repeat(4) +
      "-" +
      "0".repeat(4) +
      "-" +
      "0".repeat(12);
    await expect(acquireKey("bad-id", SCOPE)).rejects.toThrow("vault_dependency_unavailable");
    await expect(acquireKey(id, "short")).rejects.toThrow("vault_dependency_unavailable");
    await acquireKey(id, SCOPE);
    await expect(acquireKey(id, SCOPE)).rejects.toThrow("vault_dependency_unavailable");
    releaseKey(id);
    await acquireKey(id, SCOPE);
    releaseKey(id);
  });
});

describe("requestPersistence", () => {
  test("reports false without a storage manager", async () => {
    installLocks();
    await expect(requestPersistence()).resolves.toBe(false);
  });

  test("persists through the storage manager when available", async () => {
    let stored = false;
    Object.defineProperty(globalThis, "navigator", {
      value: {
        storage: {
          persisted: async () => stored,
          persist: async () => {
            stored = true;
            return true;
          },
        },
      },
      configurable: true,
    });
    await expect(requestPersistence()).resolves.toBe(true);
    installLocks();
  });
});
