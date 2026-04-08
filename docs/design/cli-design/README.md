# CLI Design

Deterministic Python CLIs in the `shared` package. Each CLI outputs JSON to stdout and diagnostics to stderr.

---

## Framework

**Typer** (`typer>=0.12`), built on Click. Type-annotated function signatures define options — no manual `@click.option` decorators. Shell completion is disabled (`add_completion=False`) on all apps since they are invoked programmatically, not interactively.

---

## Registered Commands

Declared as `[project.scripts]` in `shared/pyproject.toml`:

| Command | Entrypoint | Purpose |
|---|---|---|
| `discover` | `shared.discover:app` | Query the DDL catalog — list objects, show details, find referencing procedures |
| `init` | `shared.init:app` | Scaffold migration project files (CLAUDE.md, README.md, repo-map.json, .gitignore, .envrc, .githooks) |
| `setup-ddl` | `shared.setup_ddl:app` | Assemble DDL files and write catalog JSON from MCP query results |
| `catalog-enrich` | `shared.catalog_enrich:app` | Augment catalog with AST-derived references missed by DMF (CTAS, SELECT INTO, EXEC call chains) |
| `profile` | `shared.profile:app` | Assemble profiling context for a table + writer pair, and write profiles back to catalog |
| `migrate` | `shared.migrate:app` | Assemble migration context from catalog + profile, and write dbt model SQL + schema YAML |
| `test-harness` | `shared.test_harness:app` | Sandbox lifecycle and scenario execution for ground-truth testing |
| `migrate-util` | `shared.dry_run:app` | Dry-run and status utilities for migration pipeline |
| `generate-sources` | `shared.generate_sources:app` | Generate dbt source YAML from catalog |
| `refactor` | `shared.refactor:app` | Refactor SQL extraction and CTE restructuring |

---

## CLI Conventions

### Invocation

```bash
uv run --project lib <command> [subcommand] --option value
```

### Invocation model

CLIs are the deterministic bottom layer in a three-tier stack:

| Layer | Role | Calls CLIs? |
|---|---|---|
| **Command** | Plugin command file — accepts table names, creates worktree, spawns sub-agents, summarises results, opens PR | Spawns sub-agents |
| **Skill** | Per-item algorithm with approval gates (interactive) | Yes |
| **CLI** | Deterministic Python — JSON in/out, no LLM reasoning, no interactivity | N/A |

Sub-agents and skills call CLIs directly via `uv run`. CLIs never call skills or agents.

### I/O contract

- JSON result → `stdout`
- Warnings and progress → `stderr`
- No markdown, no explanatory text in `stdout`

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Domain failure (object not found, missing catalog, invalid input) |
| `2` | IO or parse error |

### Options, not positional arguments

All parameters use `--option` style. No CLIs use positional arguments.

---

## Subcommand Reference

### `discover`

| Subcommand | Key options | Returns |
|---|---|---|
| `list` | `--project-root`, `--type {tables,procedures,views,functions}` | `{"objects": [...]}` |
| `show` | `--project-root`, `--name schema.Name` | Object detail: columns/params/refs/statements/classification |
| `refs` | `--project-root`, `--name schema.Name` | `{"readers": [...], "writers": [...]}` |
| `write-statements` | `--project-root`, `--name schema.Name`, `--statements` (JSON array) | `{"written": ..., "statement_count": N}` |
| `write-scoping` | `--project-root`, `--name schema.Name`, `--scoping` (JSON string) | Write-back confirmation JSON |

Requires a `catalog/` directory (from `setup-ddl`). Exits with code `2` if missing.

### `init`

| Subcommand | Key options | Returns |
|---|---|---|
| `scaffold-project` | `--project-root` (optional, defaults to CWD) | `{"files_created": [...], "files_updated": [...], "files_skipped": [...]}` |
| `scaffold-hooks` | `--project-root` (optional, defaults to CWD) | `{"hook_created": bool, "hooks_path_configured": bool}` |

Both subcommands are idempotent. `scaffold-project` writes CLAUDE.md, README.md, repo-map.json, .gitignore, and .envrc. `scaffold-hooks` writes `.githooks/pre-commit` and configures `core.hooksPath`.

### `setup-ddl`

| Subcommand | Key options | Returns |
|---|---|---|
| `assemble-modules` | `--input`, `--project-root` (optional, defaults to CWD), `--type {procedures,views,functions}` | `{"file": ..., "count": N}` |
| `assemble-tables` | `--input`, `--project-root` (optional, defaults to CWD) | `{"file": ..., "count": N}` |
| `write-catalog` | `--staging-dir`, `--project-root` (optional, defaults to CWD), `--database` | Count summary JSON |
| `write-manifest` | `--project-root` (optional, defaults to CWD), `--technology`, `--database`, `--schemas` | `{"file": ...}` |

### `catalog-enrich`

Single command. Walks all procedures, extracts AST refs, and back-populates `referenced_by` on table catalogs.

| Key option | Purpose |
|---|---|
| `--project-root` | Root artifacts directory containing `ddl/`, `catalog/`, and `manifest.json` |
| `--dialect` | SQL dialect (default: `tsql`) |

Returns `{"tables_augmented": N, "procedures_augmented": N, "entries_added": N}`.

If the catalog directory is missing, returns a zeroed summary instead of exiting non-zero.

### `profile`

| Subcommand | Key options | Returns |
|---|---|---|
| `context` | `--project-root`, `--table schema.Name`, `--writer schema.Name` | Profiling context JSON (catalog signals, DDL, references) |
| `write` | `--project-root`, `--table schema.Name`, `--profile` (JSON string) | Write-back confirmation JSON |

### `migrate`

| Subcommand | Key options | Returns |
|---|---|---|
| `context` | `--project-root`, `--table schema.Name`, `--writer schema.Name` | Migration context JSON (profile, statements, DDL columns) |
| `write` | `--project-root`, `--dbt-project-path` (optional), `--table schema.Name`, `--model-sql`, `--schema-yml` | Write confirmation JSON |

### `test-harness`

| Subcommand | Key options | Returns |
|---|---|---|
| `sandbox-up` | `--project-root` | `{"status": "ok", "database": ..., "tables_cloned": N, "procedures_cloned": N}` |
| `sandbox-down` | `--project-root` | `{"status": "ok"}` |
| `sandbox-status` | `--project-root` | `{"database": ..., "exists": bool}` (schema: `sandbox_status_output.json`) |
| `execute` | `--scenario` (file path to JSON), `--project-root` | Ground-truth result JSON (schema: `test_harness_execute_output.json`) |

Reads `manifest.json` to determine technology and routes to a technology-specific backend (`sql_server`, `fabric_warehouse`). Requires `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` environment variables for SQL Server backends.

`--scenario` takes a **file path**, not inline JSON. Callers (skills) must write scenario data to a temp file before invoking `execute`.

---

## Testability Pattern

Business logic is separated from CLI wiring. Subcommands delegate to standalone functions that can be imported and tested directly without invoking Typer. CLI commands only handle I/O and exit codes. For `test-harness`, the backend abstraction (`SandboxBackend`) is the testable boundary — each method (`sandbox_up`, `sandbox_down`, `execute_scenario`) is tested independently of Typer.
