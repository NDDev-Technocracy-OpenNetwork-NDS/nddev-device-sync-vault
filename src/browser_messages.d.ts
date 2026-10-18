// This module owns only the local worker envelope. Journal commands and results
// remain opaque strings serialized and validated by the Rust journal owner.
export type JournalInput =
  | { action: "initialize"; module: string; scope: string }
  | { action: "execute"; command: string };
export type JournalRequest = JournalInput & { id: number };
export type JournalResponse =
  | { id: number; result: string }
  | { id: number; error: "vault_busy" | "vault_dependency_unavailable" };
