import { createHash } from "node:crypto";
import { lstat, readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

export const TOOL = "nds-vault-browser-store";
export interface Artifact {
  schema: 1;
  tool: typeof TOOL;
  source: string;
  baseline: string;
  wrapperSha256: string;
  files: Record<string, { bytes: number; sha256: string }>;
}
export const digest = (bytes: Uint8Array): string =>
  createHash("sha256").update(bytes).digest("hex");
export const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
export async function files(root: string, prefix = ""): Promise<string[]> {
  const output: string[] = [];
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    const name = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) output.push(...(await files(root, name)));
    else if (entry.isFile()) output.push(name);
    else throw new Error("Fixture inputs must be regular files and directories");
    if (output.length > 512) throw new Error("Too many fixture assets");
  }
  return output.sort();
}
export async function loadArtifact(
  root: string,
): Promise<{ manifest: Artifact; assets: Map<string, Uint8Array<ArrayBuffer>> }> {
  if (!(await lstat(root)).isDirectory()) throw new Error("Fixture root is redirected");
  const receiptPath = join(root, "manifest.json");
  const receiptStat = await lstat(receiptPath);
  if (!receiptStat.isFile() || receiptStat.size > 128 * 1024)
    throw new Error("Fixture receipt is redirected or too large");
  const receipt = await readFile(receiptPath);
  if (receipt.length > 128 * 1024) throw new Error("Fixture receipt exceeded its bound");
  const raw: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(receipt));
  if (
    !object(raw) ||
    raw.schema !== 1 ||
    raw.tool !== TOOL ||
    typeof raw.source !== "string" ||
    !/^[0-9a-f]{40}$/.test(raw.source) ||
    typeof raw.baseline !== "string" ||
    !/^[0-9a-f]{40}$/.test(raw.baseline) ||
    typeof raw.wrapperSha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(raw.wrapperSha256) ||
    !object(raw.files)
  )
    throw new Error("Invalid fixture receipt");
  const listed = Object.keys(raw.files).sort();
  if (
    listed.length === 0 ||
    listed.length > 512 ||
    JSON.stringify(await files(root)) !== JSON.stringify([...listed, "manifest.json"].sort())
  )
    throw new Error("Fixture asset membership differs from its receipt");
  const assets = new Map<string, Uint8Array<ArrayBuffer>>();
  let total = 0;
  for (const name of listed) {
    if (
      !/^[A-Za-z0-9_./-]+$/.test(name) ||
      name.split("/").some((part) => !part || part === "." || part === "..")
    )
      throw new Error("Invalid fixture asset path");
    const identity = raw.files[name];
    if (
      !object(identity) ||
      typeof identity.bytes !== "number" ||
      !Number.isSafeInteger(identity.bytes) ||
      identity.bytes < 0 ||
      identity.bytes > 32 * 1024 * 1024 ||
      typeof identity.sha256 !== "string" ||
      !/^[a-f0-9]{64}$/.test(identity.sha256)
    )
      throw new Error("Invalid fixture asset identity");
    const path = join(root, name);
    const stat = await lstat(path);
    if (!stat.isFile() || stat.size !== identity.bytes || total + stat.size > 128 * 1024 * 1024)
      throw new Error("Fixture asset is redirected or exceeds its declared budget");
    const bytes = Uint8Array.from(await readFile(path));
    total += bytes.length;
    if (
      bytes.length !== identity.bytes ||
      digest(bytes) !== identity.sha256 ||
      total > 128 * 1024 * 1024
    )
      throw new Error("Fixture asset differs from its receipt or size budget");
    assets.set(`/${name}`, bytes);
  }
  return { manifest: raw as unknown as Artifact, assets };
}
