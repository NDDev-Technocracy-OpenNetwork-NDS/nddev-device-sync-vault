set shell := ["bash", "-euo", "pipefail", "-c"]

check: browser-check
    cargo fmt --check
    cargo test --locked --all-features --jobs 4
    cargo clippy --locked --all-targets --all-features --jobs 4 -- -D warnings
    cargo audit --deny warnings
    cargo deny --locked check licenses sources

# The large debug acceptance future needs a bounded test-runner thread stack.
# This does not change application/runtime thread stacks or process limits.
native-acceptance:
    RUST_MIN_STACK=8388608 cargo test --locked --features development-http --lib actual_native_vault_outbox_backend_and_process_recreation -- --ignored

legacy-schema-one-acceptance:
    RUST_MIN_STACK=8388608 cargo test --locked --features development-http --lib actual_native_schema_one_unsequenced_heads_replay -- --ignored

replica-admission-acceptance:
    cargo test --locked --features development-http --lib actual_native_replica_admission_preserves_pending_and_local_reads -- --ignored

replica-owner-acceptance:
    cargo test --locked --features development-http --lib actual_native_owner_has_fresh_followers_and_hands_off -- --ignored

retained-runtime-acceptance:
    cargo test --locked --features development-http --lib actual_retained_runtime_survives_facade_cancellation_and_reuses_policy -- --ignored

# Fresh owned fixture; actual production capacity and native owner handoff.
capacity-owner-acceptance:
    cargo test --locked --features development-http --lib actual_capacity_refusal_survives_owner_handoff_without_original_rewrite -- --ignored

# Fresh owned fixture with the server's encoded-byte page budget.
large-page-acceptance:
    cargo test --locked --features development-http --lib actual_native_large_page_catchup_preserves_bounds_and_freshness -- --ignored

# Generated facts through real HTTP/PostgreSQL, then an offline SQLite reopen.
# Requires check-postgres.py --fixture-sync3 and NDS_FIXTURE_RECEIPT.
# Optimize the fixture workload while retaining the debug-only HTTP boundary.
catalog-acceptance:
    CARGO_PROFILE_TEST_OPT_LEVEL=3 cargo test --locked --features development-http --lib actual_1024_catalog_http_replay_and_offline_reopen -- --ignored --nocapture

# Linux fleet: real isolated credential services and production Rust clients.
# Precompile before starting a fresh --fixture-sync3 --fixture-seconds 3600 server.
fleet-acceptance:
    python3 scripts/fleet-acceptance.py --fixture "$NDS_FIXTURE_RECEIPT" --output "$NDS_FLEET_OUTPUT" --clients "${NDS_FLEET_CLIENTS:-32}"

# Compile gate only; real browser storage/identity acceptance is separate.
wasm-check:
    cargo clippy --locked --lib --features browser-bindings --target wasm32-unknown-unknown --jobs 4 -- -D warnings

browser-check:
    bun install --frozen-lockfile --ignore-scripts
    bun run check
    bun run check:generated
