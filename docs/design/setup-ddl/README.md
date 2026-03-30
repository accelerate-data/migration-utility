# Setup DDL Design

`setup-ddl` bootstraps the migration workspace. It connects to a live SQL Server, extracts DDL files and per-object catalog JSON, then runs offline AST enrichment. All downstream skills (`discover`, `scope`, `profile`) work from these files — no live DB required after this step.

---

## Execution modes

Two modes share the same logical flow but differ in who drives the interaction:

| Mode | Driver | SQL access | Use case |
|---|---|---|---|
| Interactive skill | Claude (SKILL.md) | `mssql` MCP tool | User-facing: schema selection, preview, confirmation |
| Python CLI | `export_ddl.py` | pyodbc / ODBC Driver 18 | Automation, CI, called by skill after confirmation |

The skill handles the user-facing steps (Steps 1–5). Once the user confirms, it calls `export_ddl.py --catalog` for the bulk extraction (Steps 6–7), then runs `catalog-enrich` offline (Step 8).

---

## Step-by-step logic

### Step 1 — Select database

Query `sys.databases` and present user databases. User picks one. All subsequent queries run in that database context.

### Step 2 — Select schemas

Query `sys.schemas` joined to `sys.objects` for object counts per schema. User picks one, several, or all schemas. Schema selection filters all subsequent queries.

### Step 3 — Extraction preview

Before writing any files, run count queries to show what will be extracted and what catalog signals are available:

```sql
-- Object counts
SELECT SUM(CASE WHEN o.type = 'U' THEN 1 ELSE 0 END) AS tables, ... FROM sys.objects o WHERE ...

-- Catalog signal availability
SELECT
  (SELECT COUNT(*) FROM sys.key_constraints WHERE type = 'PK') AS pk_count,
  (SELECT COUNT(*) FROM sys.foreign_keys) AS fk_count,
  (SELECT COUNT(*) FROM sys.identity_columns) AS identity_count,
  (SELECT COUNT(*) FROM sys.tables WHERE is_tracked_by_cdc = 1) AS cdc_count
```

Present as a summary table. User must confirm before anything is written. If declined, no files are written.

### Step 4 — Extract DDL files

Write four `.sql` files to the output directory. These are the inputs for `ddl_mcp` and sqlglot parsing:

| File | Source | Method |
|---|---|---|
| `procedures.sql` | `sys.objects` type P | `OBJECT_DEFINITION()` |
| `views.sql` | `sys.objects` type V | `OBJECT_DEFINITION()` |
| `functions.sql` | `sys.objects` types FN/IF/TF | `OBJECT_DEFINITION()` |
| `tables.sql` | `sys.tables` + `sys.columns` | Reconstructed `CREATE TABLE` from catalog |

Tables use catalog reconstruction because `OBJECT_DEFINITION()` returns null for tables. Each file uses `GO` as the statement delimiter.

### Step 5 — Extract catalog signals (bulk queries)

Bulk `SELECT` queries against `sys.*` views. No per-table loops. Results grouped by `(schema, table)` and written as per-table JSON under `catalog/tables/<schema>.<table>.json`.

| Signal | Source |
|---|---|
| `primary_keys` | `sys.key_constraints` + `sys.indexes` + `sys.index_columns` |
| `unique_indexes` | `sys.indexes` (unique, non-PK) + `sys.index_columns` |
| `foreign_keys` | `sys.foreign_keys` + `sys.foreign_key_columns` |
| `auto_increment_columns` | `sys.identity_columns` (with `seed_value`, `increment_value` cast to BIGINT — sql_variant is not ODBC-safe) |
| `cdc_enabled` | `sys.tables.is_tracked_by_cdc` |
| `change_capture` | `sys.change_tracking_tables` (graceful — view may not exist) |
| `sensitivity_classifications` | `sys.sensitivity_classifications` (graceful — requires SQL Server 2019+) |

Also builds `_build_object_type_map()`: a `{normalized_fqn: "tables"|"procedures"|"views"|"functions"}` dict from `sys.objects`. Used in the next step to resolve DMF `OBJECT_OR_COLUMN` references to the correct bucket.

### Step 6 — Extract references via DMF

Calls `sys.dm_sql_referenced_entities` for every procedure, view, and function using a server-side cursor. One batch per object type — avoids N round-trips.

```sql
SET NOCOUNT ON;  -- suppress intermediate result sets from WHILE loop
DECLARE @result TABLE (...);
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT SCHEMA_NAME(o.schema_id), o.name FROM sys.objects o WHERE o.type = 'P' ...;
OPEN cur; FETCH NEXT FROM cur INTO @schema, @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        INSERT INTO @result SELECT @schema, @name, ref.* FROM sys.dm_sql_referenced_entities(...) ref;
    END TRY BEGIN CATCH END CATCH  -- skip broken refs, don't abort
    FETCH NEXT FROM cur INTO @schema, @name;
END;
CLOSE cur; DEALLOCATE cur;
SELECT * FROM @result;
```

`SET NOCOUNT ON` is required. Without it, pyodbc sees the WHILE loop's row counts as intermediate result sets and `cursor.description` is `None` on the final `SELECT`. A `nextset()` loop advances past any remaining intermediate sets as a safety net.

**Classifying `OBJECT_OR_COLUMN` references:** DMF uses `referenced_class_desc = 'OBJECT_OR_COLUMN'` for both tables and procedures called via `EXEC`. The `_build_object_type_map()` dict resolves these — proc-to-proc EXEC calls go to the `procedures` bucket, not `tables`.

**Processing:**

1. Group rows by referencing object.
2. Classify each referenced entity into `tables`, `views`, `functions`, or `procedures` using `referenced_class_desc` + object type map.
3. Detect cross-database and cross-server references (`referenced_database_name`, `referenced_server_name`) → `out_of_scope` with reason.
4. Write per-object catalog files:
   - `catalog/procedures/<schema>.<proc>.json` — `references` with `in_scope`/`out_of_scope` per type
   - `catalog/views/<schema>.<view>.json`
   - `catalog/functions/<schema>.<function>.json`
5. Flip references: for each table/view/function in any outbound `references.*.in_scope`, add the referencing object to that target's `referenced_by` section.
6. Write `catalog/tables/<schema>.<table>.json` merging catalog signals + flipped `referenced_by`.

**Empty catalog files:** Objects with zero DMF rows (empty proc body, dynamic-SQL-only, cross-db-only references) still get catalog files — written with empty `references` arrays using the `_build_object_type_map()` key set. Tables with no constraints and no inbound refs also get files via the same map. This ensures every known object has a catalog file.

**Routing flag scan:** A regex pass over all proc/view/function bodies (`OBJECT_DEFINITION()`) sets two flags in each catalog file:

| Flag | Pattern(s) | Meaning |
|---|---|---|
| `needs_llm` | `EXEC(@var)`, `BEGIN TRY`, `WHILE`, `IF` | sqlglot cannot fully resolve — route to `discover show` / LLM |
| `needs_enrich` | `SELECT INTO`, `TRUNCATE`, static `EXEC schema.proc` | DMF left gaps; `catalog-enrich` fills them offline |

`sp_executesql` sets neither flag — DMF resolves it at definition time.

After `catalog-enrich` processes a proc, `needs_enrich` is flipped to `false`. Procs with `needs_llm: true` are skipped by `catalog-enrich` (LLM must handle them via `discover show`).

### Step 7 — AST enrichment (offline)

After `export_ddl.py` completes, run `catalog-enrich`:

```bash
uv run --project <shared-path> catalog-enrich --project-root <project-root>
```

This step has no live DB dependency. It reads `procedures.sql` and the catalog files written in Steps 5–6, then augments them with references the DMF cannot detect:

| Gap | Why DMF misses it | AST fix |
|---|---|---|
| `SELECT INTO` target | Creates a new object at runtime — not in dependency metadata | sqlglot walks `exp.Into` nodes |
| `CTAS` (CREATE TABLE AS SELECT) | Same — new object | sqlglot walks `exp.Create` with AS SELECT |
| `TRUNCATE` target | Not a dependency in DMF | sqlglot walks `exp.TruncateTable` |
| Indirect writers via EXEC chains | DMF stops at the EXEC call, doesn't follow | BFS call-graph traversal |

Augmented entries are tagged `"detection": "ast_scan"` to distinguish from DMF-sourced entries.

Dynamic SQL (`EXEC(@sql)`, `sp_executesql`) is not augmented — the target is a runtime string, unknowable offline.

### Step 8 — Write manifest

Write `manifest.json` to the output root with technology, dialect, source database, selected schemas, and extraction timestamp. Consumed by `ddl_mcp` and `discover` to set sqlglot dialect without hardcoding.

---

## Output structure

One project root = one source database. `manifest.json` at the root records which database was extracted; re-running `setup-ddl` in the same directory fully replaces `ddl/` and `catalog/`.

```text
<project-root>/
├── tables.sql
├── procedures.sql
├── views.sql
├── functions.sql
├── manifest.json
└── catalog/
    ├── tables/
    │   └── <schema>.<table>.json     ← signals + referenced_by
    ├── procedures/
    │   └── <schema>.<proc>.json      ← references (+ has_dynamic_sql if flagged)
    ├── views/
    │   └── <schema>.<view>.json      ← references
    └── functions/
        └── <schema>.<function>.json  ← references
```

Every known object gets a catalog file. The `referenced_by` on a table is populated by flipping proc/view/function outbound refs — it is always present, but may have empty arrays if nothing statically references the table.

---

## Flags: `needs_llm` and `needs_enrich`

Both flags are written by `export_ddl.py` during the body scan pass (Step 6) and stored in the catalog file. They are mutually exclusive in intent but can both be true if a proc mixes patterns (e.g. WHILE loop + SELECT INTO).

**`needs_llm`** — sqlglot cannot fully resolve the control flow. Set for:

- `EXEC(@var)` — dynamic SQL; write target is a runtime string
- `BEGIN TRY` — TRY/CATCH block; error-path DML is opaque
- `WHILE` — loop; sqlglot emits it as an opaque `Command` node
- `IF` — conditional branching; sqlglot `If` node is not fully walked

Procs with `needs_llm: true` are skipped by `catalog-enrich`. `discover show` must be used to analyze them.

**`needs_enrich`** — DMF left gaps that AST can fill. Set for:

- `SELECT INTO` — creates/writes a new table at runtime; not in DMF dependency metadata
- `TRUNCATE` — not a dependency in DMF
- Static `EXEC schema.proc` — DMF captures the proc-to-proc call but not the indirect table writes through call chains

`catalog-enrich` processes only procs where `needs_enrich: true` and `needs_llm: false`. After enrichment it flips `needs_enrich` to `false`.

| Scenario | `needs_llm` | `needs_enrich` |
|---|---|---|
| `EXEC(@sql)` | ✓ | — |
| `BEGIN TRY` / `WHILE` / `IF` | ✓ | — |
| `SELECT INTO` / `TRUNCATE` | — | ✓ |
| Static `EXEC schema.proc` | — | ✓ |
| `sp_executesql` | — | — |
| Pure DML (`INSERT`/`UPDATE`/`MERGE`/`DELETE`) | — | — |

---

## Known limitations

**Dynamic SQL is invisible offline.** `sys.dm_sql_referenced_entities` resolves references at definition time. `EXEC(@sql)` and `sp_executesql` with a string variable are unknowable without runtime capture (Query Store, Extended Events). These procs will have `has_dynamic_sql: true` in their catalog file. The tables they write to will have empty or incomplete `referenced_by`.

**Cross-database references.** DMF cannot follow references to objects in other databases. `SELECT * FROM OtherDB.dbo.T` produces no DMF rows for `OtherDB.dbo.T`. The proc catalog file will have empty `references` for these tables.

**Broken references.** If a proc references a dropped or renamed object, DMF throws an error for that proc. The `BEGIN TRY / BEGIN CATCH` in the cursor loop skips these silently — the proc gets a catalog file with empty references and no error is raised.

**Stale catalog.** If DDL changes after `setup-ddl` runs, the catalog is out of date. Re-run `setup-ddl` to refresh.
