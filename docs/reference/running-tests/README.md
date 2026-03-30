# Running Tests

How to run each test suite manually from the repository root.

## Prerequisites

| Tool | Install |
|---|---|
| Node.js 22+ | `nvm install 22` |
| Rust stable | `rustup update stable` |
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Tauri system deps (Linux) | `sudo apt-get install libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf` |

## Tauri app — Rust backend

```bash
# All Rust tests
cargo test --manifest-path app/src-tauri/Cargo.toml

# Filter by module
cargo test --manifest-path app/src-tauri/Cargo.toml db
cargo test --manifest-path app/src-tauri/Cargo.toml source_sql

# Source SQL tests that require a local SQL Server (ignored by default)
cargo test --manifest-path app/src-tauri/Cargo.toml source_sql -- --ignored
```

Run `cargo check` before committing:

```bash
cargo check --manifest-path app/src-tauri/Cargo.toml
```

## Tauri app — Frontend

```bash
# Install dependencies (first time or after lockfile changes)
cd app && npm ci

# Type check
cd app && npx tsc --noEmit

# Unit tests
cd app && npm run test:unit

# Integration tests
cd app && npm run test:integration
```

## Tauri app — E2E (Playwright)

```bash
cd app && npx playwright test
```

## ad-migration plugin — Python (discover, scope, loader)

Tests live in `tests/unit/`. They test the shared
Python library (DDL parsing, object discovery, writer detection).

```bash
# From repo root — all migration tests
uv run \
  --project lib \
  --extra dev \
  python -m pytest tests/unit/ -v

# Single test file
uv run \
  --project lib \
  --extra dev \
  python -m pytest tests/unit/test_discover.py -v

# Single test
uv run \
  --project lib \
  --extra dev \
  python -m pytest tests/unit/test_smoke.py::test_load_directory_mixed_types_single_file -v
```

The `--project` flag tells uv which virtual environment to use. The
`--extra dev` flag installs pytest (a dev dependency). Without both,
imports will fail with `ModuleNotFoundError`.

## ad-migration plugin — Scoping agent (vitest)

Tests live in `tests/agents/`. They run the
scoping agent via the Claude Code CLI against DDL fixture directories.

```bash
# From repo root
cd tests && npx vitest run
```

These tests require the `claude` CLI on PATH and make live API calls.
Do not run in CI — run manually only.

## ad-migration plugin — CLI tools

The `discover` and `scope` CLIs can be run directly for manual
verification:

```bash
# List tables in a DDL directory
uv run \
  --project lib \
  discover list --ddl-path <path-to-sql-files> --type tables

# Show details for a specific object
uv run \
  --project lib \
  discover show --ddl-path <path-to-sql-files> --name silver.DimProduct

# Find writer procedures for a table
uv run \
  --project lib \
  scope --ddl-path <path-to-sql-files> --table silver.DimProduct
```

`--ddl-path` must point to a directory containing `.sql` files, not to
a single file. Object types are auto-detected from `CREATE` statements
inside the files — filenames are not significant.

## Quick reference

| What changed | Command |
|---|---|
| Rust command or `db.rs` | `cargo test --manifest-path app/src-tauri/Cargo.toml <module>` |
| Frontend store / hook | `cd app && npm run test:unit` |
| Frontend component / page | `cd app && npm run test:integration` |
| E2E flow | `cd app && npx playwright test` |
| Python shared lib (loader, discover, scope) | `uv run --project lib --extra dev python -m pytest tests/unit/` |
| Scoping agent (manual only) | `cd tests && npx vitest run` |
| Type check (always run before commit) | `cd app && npx tsc --noEmit` |
| Rust check (always run before commit) | `cargo check --manifest-path app/src-tauri/Cargo.toml` |
