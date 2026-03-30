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
| `setup-ddl` | `shared.setup_ddl:app` | Assemble DDL files and write catalog JSON from MCP query results |
| `catalog-enrich` | `shared.catalog_enrich:app` | Augment catalog with AST-derived references missed by DMF (CTAS, SELECT INTO, EXEC call chains) |

---

## CLI Conventions

### Invocation

Agents invoke CLIs via `uv run`:

```bash
uv run --project agent-sources/ad-migration/workbench/migration/shared <command> [subcommand] --option value
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
| `list` | `--ddl-path`, `--type {tables,procedures,views,functions}` | `{"objects": [...]}` |
| `show` | `--ddl-path`, `--name schema.Name` | Object detail: columns/params/refs/statements/classification |
| `refs` | `--ddl-path`, `--name schema.Name` | `{"readers": [...], "writers": [...]}` |

Requires a `catalog/` directory (from `setup-ddl`). Exits with code `2` if missing.

### `setup-ddl`

| Subcommand | Key options | Returns |
|---|---|---|
| `assemble-modules` | `--input`, `--output-folder`, `--type {procedures,views,functions}` | `{"file": ..., "count": N}` |
| `assemble-tables` | `--input`, `--output-folder` | `{"file": ..., "count": N}` |
| `write-catalog` | `--staging-dir`, `--output-folder`, `--database` | Count summary JSON |
| `write-manifest` | `--output-folder`, `--technology`, `--database`, `--schemas` | `{"file": ...}` |

### `catalog-enrich`

Single command. Walks all procedures, extracts AST refs, and back-populates `referenced_by` on table catalogs.

| Key option | Purpose |
|---|---|
| `--ddl-path` | Root artifacts directory containing `ddl/`, `catalog/`, and `manifest.json` |
| `--dialect` | SQL dialect (default: `tsql`) |

Returns `{"tables_augmented": N, "procedures_augmented": N, "entries_added": N}`.

---

## Testability Pattern

Business logic is separated from CLI wiring. Each subcommand delegates to a standalone function (e.g., `run_list`, `run_show`, `run_refs`, `enrich_catalog`) that can be imported and tested directly without invoking Typer. CLI commands only handle I/O and exit codes.
