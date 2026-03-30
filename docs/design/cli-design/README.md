# CLI Design

Deterministic Python CLIs in the `shared` package that agents call via `uv run`. Each CLI outputs JSON to stdout and diagnostics to stderr.

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

---

## CLI Conventions

### Invocation

Agents invoke CLIs via `uv run`:

```bash
uv run --project lib <command> [subcommand] --option value
```

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

---

## Testability Pattern

Business logic is separated from CLI wiring. Subcommands delegate to standalone functions (e.g., `run_list`, `run_show`, `run_refs`, `run_write_statements`, `enrich_catalog`, `run_context`, `run_write`, `run_scaffold_project`, `run_scaffold_hooks`, `run_assemble_modules`, `run_assemble_tables`, `run_write_catalog`, `run_write_manifest`) that can be imported and tested directly without invoking Typer. CLI commands only handle I/O and exit codes.
