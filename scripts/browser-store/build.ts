import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { type Artifact, digest, files, loadArtifact, object, TOOL } from "./artifact";

const root = resolve(import.meta.dir, "../..");
const output = resolve(process.argv[2] ?? join(root, ".nddev-reports/browser-store"));
async function run(argv: string[], cwd: string, seconds = 120): Promise<string> {
  const child = Bun.spawn(argv, { cwd, stdout: "pipe", stderr: "inherit", detached: true });
  const stop = () => {
    if (child.exitCode === null) {
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        child.kill("SIGKILL");
      }
    }
  };
  const timer = setTimeout(stop, seconds * 1000);
  const captured = async (): Promise<string> => {
    const reader = child.stdout.getReader();
    const chunks: Uint8Array[] = [];
    let length = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.length;
      if (length > 16 * 1024 * 1024) throw new Error("Build output exceeded its bound");
      chunks.push(value);
    }
    const buffer = new Uint8Array(length);
    let cursor = 0;
    for (const chunk of chunks) {
      buffer.set(chunk, cursor);
      cursor += chunk.length;
    }
    return new TextDecoder("utf-8", { fatal: true }).decode(buffer);
  };
  try {
    const [text, status] = await Promise.all([captured(), child.exited]);
    if (status !== 0) {
      // Cargo JSON carries the actual diagnostic on stdout, not stderr.
      // Preserve it when collecting artifact paths instead of losing the cause.
      if (argv.includes("--message-format=json")) {
        for (const line of text.split("\n")) {
          if (!line.trim()) continue;
          const message: unknown = JSON.parse(line);
          if (
            object(message) &&
            message.reason === "compiler-message" &&
            object(message.message) &&
            typeof message.message.rendered === "string"
          )
            console.error(message.message.rendered);
        }
      }
      throw new Error(`Fixture command failed: ${argv[0]}`);
    }
    return text.trim();
  } catch (error) {
    stop();
    await child.exited;
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
const source = await run(["git", "rev-parse", "HEAD"], root);
if (!/^[a-f0-9]{40}$/.test(source) || (await run(["git", "status", "--porcelain"], root)))
  throw new Error("Build the browser fixture from committed, clean sources");
const baseline: unknown = JSON.parse(
  await readFile(join(import.meta.dir, "baseline.json"), "utf8"),
);
if (
  !object(baseline) ||
  baseline.schema !== 1 ||
  typeof baseline.source !== "string" ||
  !/^[a-f0-9]{40}$/.test(baseline.source)
)
  throw new Error("Invalid retained source identity");
try {
  const existing = await loadArtifact(output);
  if (existing.manifest.source === source && existing.manifest.baseline === baseline.source) {
    console.log(JSON.stringify({ reused: true, source, output }));
    process.exit(0);
  }
  throw new Error("Preserve the previous fixture; select a fresh output directory");
} catch (error) {
  if (!(object(error) && error.code === "ENOENT" && error.path === output)) throw error;
}
await mkdir(dirname(output), { recursive: true });
await mkdir(output); // Claim a fresh directory; never replace unknown output.
const temporary = await mkdtemp(join(tmpdir(), "nds-vault-browser-build-"));
try {
  const previous = join(temporary, "baseline");
  await mkdir(previous);
  const archive = join(temporary, "baseline.tar");
  await run(["git", "archive", "--format=tar", "--output", archive, baseline.source], root);
  await run(["tar", "-xf", archive, "-C", previous], root);
  const manifest: unknown = Bun.TOML.parse(await readFile(join(root, "Cargo.toml"), "utf8"));
  if (!object(manifest) || !Array.isArray(manifest.example))
    throw new Error("Missing example target");
  const example = manifest.example.find(
    (entry: unknown) => object(entry) && entry.name === "browser-journal",
  );
  if (
    !object(example) ||
    example.path !== "examples/browser-journal.rs" ||
    JSON.stringify(Object.keys(example).sort()) !==
      JSON.stringify(["crate-type", "name", "path", "required-features"]) ||
    JSON.stringify(example["crate-type"]) !== JSON.stringify(["cdylib"]) ||
    JSON.stringify(example["required-features"]) !== JSON.stringify(["browser-bindings"])
  )
    throw new Error("Invalid example target");
  const wrapper = await readFile(join(root, example.path));
  await mkdir(join(previous, "examples"), { recursive: true });
  await writeFile(join(previous, example.path), wrapper, { flag: "wx" });
  let priorManifest = await readFile(join(previous, "Cargo.toml"), "utf8");
  if ((priorManifest.match(/^\[dev-dependencies\]$/gm) ?? []).length !== 1)
    throw new Error("Unexpected retained manifest");
  priorManifest = priorManifest.replace(
    "[dev-dependencies]",
    "[target.'cfg(not(target_arch = \"wasm32\"))'.dev-dependencies]",
  );
  // Derive only the example descriptor and native-only test dependency gate.
  // The retained library sources and dependency lock stay byte-for-byte intact.
  priorManifest += `\n[[example]]\n${Object.entries(example)
    .map(([key, value]) => `${key} = ${JSON.stringify(value)}`)
    .join("\n")}\n`;
  await writeFile(join(previous, "Cargo.toml"), priorManifest);
  if (
    digest(await readFile(join(previous, "src/browser_worker.js"))) !==
    digest(await readFile(join(root, "src/browser_worker.js")))
  )
    throw new Error("The retained worker protocol needs its own transition fixture");
  const bindingVersion = async (directory: string): Promise<string> => {
    const lock: unknown = Bun.TOML.parse(await readFile(join(directory, "Cargo.lock"), "utf8"));
    if (!object(lock) || !Array.isArray(lock.package)) throw new Error("Invalid compiler lock");
    const bindings = lock.package.filter(
      (entry: unknown) => object(entry) && entry.name === "wasm-bindgen",
    );
    const binding: unknown = bindings[0];
    if (
      bindings.length !== 1 ||
      !object(binding) ||
      typeof binding.version !== "string" ||
      !/^\d+\.\d+\.\d+$/.test(binding.version)
    )
      throw new Error("Missing matched binding version");
    return binding.version;
  };
  const binding = await bindingVersion(root);
  if ((await bindingVersion(previous)) !== binding)
    throw new Error("The retained artifact needs its own matched compiler");
  const retainedLock = digest(await readFile(join(previous, "Cargo.lock")));
  await run(
    [
      "cargo",
      "install",
      "--locked",
      "--root",
      join(temporary, "tools"),
      "--version",
      binding,
      "wasm-bindgen-cli",
    ],
    root,
    600,
  );
  const bindgen = join(temporary, "tools/bin/wasm-bindgen");
  for (const [version, directory] of [
    ["baseline", previous],
    ["candidate", root],
  ] as const) {
    const messages = await run(
      [
        "cargo",
        "build",
        "--locked",
        "--release",
        "--target",
        "wasm32-unknown-unknown",
        "--features",
        "browser-bindings",
        "--example",
        "browser-journal",
        "--message-format=json",
      ],
      directory,
      600,
    );
    let wasm: string | undefined;
    for (const line of messages.split("\n")) {
      const message: unknown = JSON.parse(line);
      if (
        !object(message) ||
        message.reason !== "compiler-artifact" ||
        !object(message.target) ||
        message.target.name !== "browser-journal" ||
        !Array.isArray(message.filenames)
      )
        continue;
      for (const name of message.filenames)
        if (typeof name === "string" && name.endsWith(".wasm")) wasm = name;
    }
    if (!wasm) throw new Error("Cargo did not report the actual browser artifact");
    await run(
      [
        bindgen,
        "--target",
        "web",
        "--out-dir",
        join(output, version),
        "--out-name",
        "journal_fixture",
        wasm,
      ],
      directory,
    );
  }
  if (digest(await readFile(join(previous, "Cargo.lock"))) !== retainedLock)
    throw new Error("The retained dependency lock changed");
  if (await run(["git", "status", "--porcelain"], root))
    throw new Error("Fixture compilation changed its source inputs");
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "page.ts")],
    target: "browser",
    minify: false,
  });
  if (!built.success || built.outputs.length !== 1)
    throw new Error("Browser fixture bundling failed");
  const script = built.outputs[0];
  if (!script) throw new Error("Missing compiled fixture page");
  await writeFile(join(output, "fixture.js"), await script.text(), { flag: "wx" });
  for (const name of ["index.html", "fixture.css"])
    await writeFile(join(output, name), await readFile(join(import.meta.dir, name)), {
      flag: "wx",
    });
  await writeFile(
    join(output, "browser_worker.js"),
    await readFile(join(root, "src/browser_worker.js")),
    { flag: "wx" },
  );
  const receipt: Artifact = {
    schema: 1,
    tool: TOOL,
    source,
    baseline: baseline.source,
    wrapperSha256: digest(wrapper),
    files: {},
  };
  for (const name of await files(output)) {
    const bytes = await readFile(join(output, name));
    receipt.files[name] = { bytes: bytes.length, sha256: digest(bytes) };
  }
  await writeFile(join(output, "manifest.json"), `${JSON.stringify(receipt, null, 2)}\n`, {
    flag: "wx",
  });
  await loadArtifact(output);
  console.log(JSON.stringify({ source, baseline: baseline.source, output }));
} finally {
  await rm(temporary, { recursive: true });
}
