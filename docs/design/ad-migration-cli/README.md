# ad-migration CLI

Standalone `ad-migration` CLI for deterministic pipeline commands, distributed via Homebrew and pip. Replaces the corresponding plugin commands so the plugin retains only LLM-driven work.

## Decision

Deterministic commands (init, extract, target/sandbox setup, pipeline state mutations) have no LLM reasoning — they are pure orchestration over existing Python CLIs. Packaging them as a standalone CLI lets users run them from scripts and CI without launching Claude Code, and distributes them through standard tooling (Homebrew, pip).

## Architecture

New `lib/shared/cli/` package: a thin human-facing layer that calls the same `run_*` functions the existing agent-facing CLIs call. Existing CLIs (`migrate-util`, `setup-ddl`, `init`, `test-harness`) are unchanged — they remain agent-facing and emit JSON. The new layer formats output for humans using `rich`.

```text
lib/shared/cli/
  main.py               # top-level Typer app
  setup_source_cmd.py
  setup_target_cmd.py   # new Python backing (no prior CLI)
  setup_sandbox_cmd.py
  teardown_sandbox_cmd.py
  reset_cmd.py
  exclude_table_cmd.py
  add_source_table_cmd.py
  output.py             # shared rich formatting helpers
  env_check.py          # shared env var validation
```

New entrypoint in `lib/pyproject.toml`:

```toml
ad-migration = "shared.cli.main:app"
```

Dev usage (no install required):

```bash
uv run --project lib ad-migration <command>
```

## User flow

```text
1. Install plugin from Claude Code marketplace
2. Run /init-ad-migration (plugin command) — installs CLI + checks prereqs + scaffolds project
3. Run ad-migration setup-source / setup-target / setup-sandbox (CLI)
4. Run /scope, /profile, /generate-model etc. (plugin, LLM-driven)
```

`init-ad-migration` is the only plugin command that acts as a bootstrap — it is the single entry point that installs the `ad-migration` CLI via Homebrew, validates prerequisites, and scaffolds the project. After that, the CLI takes over for all deterministic setup work.

## CLI commands

| Command | Flags | Env vars read |
|---|---|---|
| `setup-source` | `--technology sql_server\|oracle` `--schemas silver,gold` `[--no-commit]` | `MSSQL_*` / `ORACLE_*` |
| `setup-target` | `--technology fabric\|snowflake\|duckdb` `[--source-schema bronze]` `[--no-commit]` | `TARGET_*` (technology-specific) |
| `setup-sandbox` | `[--yes]` | Reads `runtime.sandbox` from manifest + sandbox credential vars |
| `teardown-sandbox` | `[--yes]` | Reads `runtime.sandbox` from manifest |
| `reset` | `<scope\|profile\|generate-tests\|refactor>` `<fqn> [fqn ...]` `[--yes]` | None |
| `exclude-table` | `<fqn> [fqn ...]` `[--no-commit]` | None |
| `add-source-table` | `<fqn> [fqn ...]` `[--no-commit]` | None |

Technology ownership:

- `setup-source --technology` validates source env vars and runs extraction.
- `setup-target --technology` owns target env var validation and dbt scaffolding.
- `setup-sandbox` infers technology from manifest (set by `setup-source`).

## Env var contract

Every command validates all required env vars at startup via `env_check.py`. On failure, exit code 1 with a single message listing every missing var and what to do:

```text
Error: missing required environment variables for sql_server:

  MSSQL_HOST    not set
  SA_PASSWORD   not set

Set these in your shell or .envrc before running setup-source.
```

Never fail mid-command on a missing var.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Domain failure (bad args, missing prereq, validation error) |
| 2 | IO / connection error |

## Output

Human-readable `rich` output to stdout. Spinners for long-running steps (extraction, `dbt compile`). Summary table at completion. `--quiet` suppresses all output except errors, for CI use.

## Plugin evolution

Remove plugin commands whose logic now lives in the CLI or in scripts:

- `setup-ddl`, `setup-target`, `setup-sandbox`, `teardown-sandbox`, `reset-migration`, `exclude-table`, `add-source-tables`, `commit`, `commit-push-pr`

Plugin retains `init-ad-migration` (bootstrap: installs CLI, checks prereqs, scaffolds project) and LLM-driven commands: `scope`, `profile`, `generate-model`, `generate-tests`, `refactor`, `status`.

When a skill or rule needs to invoke a deterministic step mid-workflow, it calls `ad-migration <command>` via bash.

## Scripts

Git workflow helpers stay as shell scripts, not CLI commands — same pattern as `scripts/worktree.sh`:

- `scripts/cleanup-worktrees.sh` — scans worktrees, checks merged PRs via `gh`, removes merged ones.
- `scripts/commit.sh` — stage specified files and commit with a provided message.
- `scripts/commit-push-pr.sh` — stage, commit, push, open PR via `gh pr create`.

Claude rules reference these directly.

## Distribution

**Development:** `uv run --project lib ad-migration <command>` — no install needed.

**PyPI:** `lib/` package published as `ad-migration`. Entrypoint registered in `pyproject.toml`. Published via GitHub Actions on tag push.

**Homebrew:** Tap at `accelerate-data/homebrew-tap`. Formula uses `uv tool install ad-migration`. Users install with:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
```

**Plugin marketplace:** Distributed independently. The `install-cli` plugin command is a soft check with instructions — not an auto-installer. Users can use LLM plugin commands without the CLI if they are not running setup or state-mutation steps.
