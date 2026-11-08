type ObjectValue = Record<string, unknown>;
const object = (value: unknown): value is ObjectValue =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const required = (condition: unknown, message: string): void => {
  if (!condition) throw new Error(message);
};
function equal(left: unknown, right: unknown, message: string): void {
  required(JSON.stringify(left) === JSON.stringify(right), message);
}
function success(raw: unknown): unknown {
  required(typeof raw === "string", "Worker returned a non-string envelope");
  const value: unknown = JSON.parse(raw as string);
  if (!object(value) || Object.keys(value).length !== 1 || !("Ok" in value))
    throw new Error("Journal refused the fixture operation");
  return value.Ok;
}
function field(value: unknown, name: string): unknown {
  if (!object(value) || !(name in value)) throw new Error("Unexpected journal reply");
  return value[name];
}

class Client {
  readonly worker = new Worker("/browser_worker.js", { type: "module" });
  private sequence = 0;
  private pending = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: Error) => void; timer: number }
  >();
  constructor() {
    this.worker.onmessage = ({ data }: MessageEvent<unknown>) => {
      if (!object(data) || typeof data.id !== "number") return;
      const call = this.pending.get(data.id);
      if (!call) return;
      this.pending.delete(data.id);
      clearTimeout(call.timer);
      if ("error" in data) call.reject(new Error("Journal worker rejected the request"));
      else call.resolve(data.result);
    };
    this.worker.onerror = () => this.close();
  }
  async message(data: ObjectValue): Promise<unknown> {
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.pending.delete(id);
        reject(new Error("Journal worker response timed out"));
      }, 10_000);
      this.pending.set(id, { resolve, reject, timer });
      this.worker.postMessage({ id, ...data });
    });
  }
  async open(version: string, scope: string): Promise<void> {
    equal(
      await this.message({
        action: "initialize",
        module: new URL(`/${version}/journal_fixture.js`, location.href).href,
        scope,
      }),
      "ready",
      "Worker did not acknowledge its paused VFS",
    );
  }
  async execute(command: unknown): Promise<unknown> {
    return this.message({ action: "execute", command: JSON.stringify(command) });
  }
  close(): void {
    for (const call of this.pending.values()) {
      clearTimeout(call.timer);
      call.reject(new Error("Owned fixture worker closed"));
    }
    this.pending.clear();
    this.worker.terminate();
  }
}

async function run(): Promise<ObjectValue> {
  const scope = Array.from(crypto.getRandomValues(new Uint8Array(32)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  const moduleUrl = new URL("/candidate/journal_fixture.js", location.href).href;
  const module: unknown = await import(moduleUrl);
  if (
    !object(module) ||
    typeof module.default !== "function" ||
    typeof module.fixture_originals !== "function"
  )
    throw new Error("Invalid fixture composition");
  await module.default();
  const encoded: unknown = module.fixture_originals();
  if (typeof encoded !== "string") throw new Error("Missing opaque fixture rows");
  const originals: unknown = JSON.parse(encoded);
  if (
    !Array.isArray(originals) ||
    originals.length !== 3 ||
    !originals.every(
      (row: unknown) =>
        Array.isArray(row) &&
        row.length > 0 &&
        row.every(
          (byte: unknown) => Number.isInteger(byte) && Number(byte) >= 0 && Number(byte) <= 255,
        ),
    )
  )
    throw new Error("Invalid opaque fixture rows");
  const enqueue = (body: unknown): ObjectValue => ({
    Enqueue: { body, expected_cursor: null, expected_revision: 0, expected_pending: null },
  });
  const clients: Client[] = [];
  let result: ObjectValue | undefined;
  let failure: unknown;
  const open = async (version: string): Promise<Client> => {
    const client = new Client();
    clients.push(client);
    await client.open(version, scope);
    return client;
  };
  try {
    const old = await open("baseline");
    equal(
      field(success(await old.execute("Stats")), "Stats"),
      [0, 0, 0, 0, 0],
      "Fixture store was not empty",
    );
    success(await old.execute(enqueue(originals[0])));
    success(await old.execute(enqueue(originals[1])));
    const retained = field(success(await old.execute("Pending")), "Pending");
    required(Array.isArray(retained) && retained.length === 2, "Two originals were not claimed");
    const before = field(success(await old.execute("Stats")), "Stats");
    old.close();

    const current = await open("candidate");
    equal(
      field(success(await current.execute("Stats")), "Stats"),
      before,
      "Upgrade changed journal metadata",
    );
    equal(
      field(success(await current.execute("Pending")), "Pending"),
      retained,
      "Upgrade changed original bytes or their claim",
    );
    const refused: unknown = JSON.parse(String(await current.execute(enqueue([0]))));
    required(object(refused) && "Err" in refused, "Malformed original was admitted");
    equal(
      field(success(await current.execute("Stats")), "Stats"),
      before,
      "Refused input mutated the journal",
    );
    success(await current.execute(enqueue(originals[2])));
    const after = field(success(await current.execute("Pending")), "Pending");
    required(Array.isArray(after) && after.length === 3, "New writer lost retained originals");
    const metadata = field(success(await current.execute("Stats")), "Stats");
    current.close();

    const reopenedOld = await open("baseline");
    equal(
      field(success(await reopenedOld.execute("Pending")), "Pending"),
      after,
      "Previous VFS could not reopen the unchanged journal format",
    );
    equal(
      field(success(await reopenedOld.execute("Stats")), "Stats"),
      metadata,
      "Previous VFS changed committed metadata",
    );
    reopenedOld.close();

    const final = await open("candidate");
    const peer = await open("candidate");
    const views = await Promise.all([final.execute("Stats"), peer.execute("Stats")]);
    for (const view of views)
      equal(field(success(view), "Stats"), metadata, "Independent workers disagreed");
    equal(
      field(success(await final.execute("Pending")), "Pending"),
      after,
      "Final restart changed exact original bytes",
    );
    result = {
      status: "passed",
      phases: ["baseline", "candidate", "baseline", "candidate"],
      originals: 3,
      independentWorkers: 2,
    };
  } catch (error) {
    failure = error;
  } finally {
    for (const client of clients) client.close();
    try {
      const root = await navigator.storage.getDirectory();
      const directory = await root.getDirectoryHandle("nds-replica");
      await directory.removeEntry(scope, { recursive: true });
    } catch (error) {
      if (!(error instanceof DOMException && error.name === "NotFoundError")) failure ??= error;
    }
  }
  if (failure !== undefined) throw failure;
  if (!result) throw new Error("Missing fixture outcome");
  return result;
}

declare global {
  interface Window {
    __journalFixture?: ObjectValue;
  }
}
const status = document.getElementById("status");
const output = document.getElementById("result");
if (!status || !output) throw new Error("Missing fixture result surface");
run()
  .then((result) => {
    window.__journalFixture = result;
    status.textContent = "Passed";
    output.textContent = JSON.stringify(result, null, 2);
  })
  .catch((error: unknown) => {
    const result = {
      status: "failed",
      error: error instanceof Error ? error.message : "Fixture failed",
    };
    window.__journalFixture = result;
    status.textContent = "Failed";
    output.textContent = JSON.stringify(result, null, 2);
  });

export {};
