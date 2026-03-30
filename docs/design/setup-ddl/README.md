# Setup DDL Design

`setup-ddl` bootstraps the migration workspace. It connects to a live SQL Server via the `mssql` MCP tool, extracts DDL files and per-object catalog JSON, then runs offline AST enrichment. All downstream skills (`discover`, `scope`, `profile`) work from these local files — no live DB required after this step.

---

## Architecture

The skill (`bootstrap/skills/setup-ddl/SKILL.md`) drives the interaction. It runs SQL via the `mssql` MCP tool, saves raw results as JSON to `.staging/`, then calls the `setup-ddl` CLI (`lib/shared/setup_ddl.py`) for deterministic file processing. The agent never processes query results inline — it acts as a relay between MCP and CLI.

```text
User ← Agent (SKILL.md) → mssql MCP → SQL Server
                        ↓
                   .staging/*.json
                        ↓
              setup-ddl CLI (run_* functions)
                        ↓
              ddl/*.sql + catalog/**/*.json + manifest.json
                        ↓
              catalog-enrich CLI (offline AST pass)
```

---

## Step-by-step logic

### Step 1 — Select database

Query `sys.databases` via MCP and present user databases. User picks one. All subsequent queries run in that database context (`USE [<database>]`).

If `manifest.json` already exists, the database is locked to `source_database` from the manifest — skip to Step 2.

### Step 2 — Select schemas

Query `sys.schemas` joined to `sys.objects` for object counts per schema. User picks one, several, or all schemas. Schema selection filters all subsequent queries.

### Step 3 — Extraction preview + confirm

Count queries show what will be extracted and which catalog signals are available (PKs, FKs, identity columns, CDC-tracked tables). User must confirm before anything is written.

### Step 4 — Write manifest

First file written, immediately after confirmation:

```bash
uv run --project <shared-path> setup-ddl write-manifest \
  --technology sql_server --database <database> --schemas <schemas>
```

Records technology, dialect, source database, selected schemas, and extraction timestamp. Consumed by `ddl_mcp` and `discover` to set the sqlglot dialect.

### Step 5 — Export procedures, views, and functions

For each object type: run `OBJECT_DEFINITION()` query via MCP, save result to `.staging/<type>.json`, then call the CLI:

```bash
uv run --project <shared-path> setup-ddl assemble-modules \
  --input .staging/procedures.json --type procedures
```

Repeat for views and functions. Each produces a GO-delimited `.sql` file in `ddl/`.

### Step 6 — Export tables

Tables use catalog reconstruction (columns from `sys.columns` + `sys.types`) because `OBJECT_DEFINITION()` returns null for tables. Save column metadata to `.staging/table_columns.json`, then:

```bash
uv run --project <shared-path> setup-ddl assemble-tables \
  --input .staging/table_columns.json
```

Produces `ddl/tables.sql` with reconstructed `CREATE TABLE` statements, GO-delimited.

### Step 7 — Extract catalog signals and references

All remaining catalog queries run via MCP and save results to `.staging/`:

| Staging file | Content |
|---|---|
| `pk_unique.json` | Primary keys and unique indexes |
| `foreign_keys.json` | Foreign key constraints |
| `identity_columns.json` | Identity columns |
| `cdc.json` | CDC-tracked tables |
| `change_tracking.json` | Change tracking tables (graceful — view may not exist) |
| `sensitivity.json` | Sensitivity classifications (graceful — requires SQL Server 2019+) |
| `object_types.json` | Object type map (`{schema, name, type}` for all objects) |
| `definitions.json` | All proc/view/function bodies (for routing flag scan) |
| `proc_params.json` | Procedure parameters |
| `proc_dmf.json` | DMF refs for procedures (server-side cursor batch) |
| `view_dmf.json` | DMF refs for views |
| `func_dmf.json` | DMF refs for functions |

DMF queries use server-side cursors to batch `sys.dm_sql_referenced_entities` calls into one result set per object type, with `BEGIN TRY / BEGIN CATCH` to skip broken refs.

Once all staging files are saved, one CLI call processes everything:

```bash
uv run --project <shared-path> setup-ddl write-catalog \
  --staging-dir .staging --database <database>
```

The `run_write_catalog` function:

1. Builds table signals from column, PK, FK, identity, CDC, change tracking, and sensitivity data.
2. Builds an object type map (`_build_object_types_map`) to resolve `OBJECT_OR_COLUMN` DMF references into the correct bucket (tables vs procedures).
3. Scans all definitions for routing flags (`scan_routing_flags`).
4. Calls `write_catalog_files` to write per-object catalog JSON, including reference flipping (proc outbound refs → table `referenced_by`).

### Step 8 — AST enrichment (offline)

No live DB dependency. Reads `ddl/procedures.sql` and catalog files, augments with references the DMF cannot detect:

```bash
uv run --project <shared-path> catalog-enrich --project-root .
```

| Gap | Why DMF misses it | AST fix |
|---|---|---|
| `SELECT INTO` target | Creates a new object at runtime | sqlglot walks `exp.Into` nodes |
| `CTAS` | Same — new object | sqlglot walks `exp.Create` with AS SELECT |
| `TRUNCATE` target | Not a dependency in DMF | sqlglot walks `exp.TruncateTable` |
| Indirect writers via EXEC chains | DMF stops at the EXEC call | BFS call-graph traversal |

Augmented entries are tagged `"detection": "ast_scan"`. Dynamic SQL (`EXEC(@sql)`, `sp_executesql`) is not augmented — the target is a runtime string, unknowable offline.

### Step 9 — Report

Summary of files written (DDL counts, catalog counts, manifest path). Tells the user they can now run `discover` or the `scoping-agent`.

---

## Output structure

One project root = one source database. Re-running `setup-ddl` fully replaces `ddl/` and `catalog/`.

```text
<project-root>/
├── manifest.json
├── ddl/
│   ├── tables.sql
│   ├── procedures.sql
│   ├── views.sql
│   └── functions.sql
└── catalog/
    ├── tables/
    │   └── <schema>.<table>.json     ← signals + referenced_by
    ├── procedures/
    │   └── <schema>.<proc>.json      ← references + routing flags
    ├── views/
    │   └── <schema>.<view>.json      ← references
    └── functions/
        └── <schema>.<function>.json  ← references
```

Every known object gets a catalog file. `referenced_by` on a table is populated by flipping proc/view/function outbound refs — always present, but may have empty arrays.

---

## Routing flags: `needs_llm` and `needs_enrich`

Written by `setup-ddl write-catalog` during the routing flag scan (`scan_routing_flags` in `catalog.py`). Stored in each proc/view/function catalog file.

**`needs_llm`** — sqlglot cannot fully resolve the control flow:

| Pattern | Regex | Why |
|---|---|---|
| `EXEC(@var)` / `EXECUTE(@var)` | `\bEXEC(?:UTE)?\s*\(` | Dynamic SQL; write target is a runtime string |
| `BEGIN TRY` | `\bBEGIN\s+TRY\b` | TRY/CATCH block; error-path DML is opaque |
| `WHILE` | `\bWHILE\b` | Loop; sqlglot emits as opaque `Command` node |
| `IF` | `\bIF\b` | Conditional branching; sqlglot `If` node not fully walked |

**`needs_enrich`** — DMF left gaps that AST can fill:

| Pattern | Regex | Why |
|---|---|---|
| `SELECT INTO` (not `INSERT INTO`) | `^(?!.*\bINSERT\b).*\bINTO\s+[\[\w#@]` (multiline) | Creates/writes a new table; not in DMF |
| `TRUNCATE` | `\bTRUNCATE\b` | Not a dependency in DMF |
| Static `EXEC schema.proc` | `\bEXEC(?:UTE)?\s+(?!sp_executesql\b)(?![@(])[\[\w]` | DMF captures the call but not indirect table writes |

`sp_executesql` sets neither flag — DMF resolves it at definition time.

`catalog-enrich` processes only entries where `needs_enrich: true` and `needs_llm: false`. After enrichment it flips `needs_enrich` to `false`. Entries with `needs_llm: true` require LLM analysis via `discover show`.

---

## Known limitations

**Dynamic SQL is invisible offline.** `sys.dm_sql_referenced_entities` resolves references at definition time. `EXEC(@sql)` and `sp_executesql` with a string variable are unknowable without runtime capture. These procs have `needs_llm: true`. Tables they write to will have empty or incomplete `referenced_by`.

**Cross-database references.** DMF cannot follow references to objects in other databases. These are classified as `out_of_scope` with a reason in the proc catalog file.

**Broken references.** If a proc references a dropped or renamed object, DMF throws an error. The `BEGIN TRY / BEGIN CATCH` in the cursor loop skips these — the proc gets a catalog file with empty references.

**Stale catalog.** If DDL changes after `setup-ddl` runs, the catalog is out of date. Re-run `setup-ddl` to refresh.
