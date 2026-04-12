# Refactoring SQL Skill Contract

The refactoring-sql skill converts raw T-SQL stored procedure (or view) SQL into a structured import/logical/final CTE pattern. It uses two isolated sub-agents to produce independent outputs, then proves equivalence by executing both against the sandbox. The output stays in T-SQL — dbt Jinja conversion happens in the downstream `generating-model` skill.

## Inputs

| Field | Source | Required |
|---|---|---|
| `table_fqn` | Skill argument (`$ARGUMENTS`) | yes |
| `selected_writer` | `catalog/tables/<table>.json` → `scoping.selected_writer` | yes (proc path) |
| `proc_body` | `catalog/procedures/<writer>.json` | yes (proc path) |
| `statements` | `catalog/procedures/<writer>.json` | yes (proc path) |
| `view_sql` | `catalog/views/<table>.json` | yes (view path) |
| `profile` | `catalog/tables/<table>.json` → profile section | yes |
| `columns` | catalog DDL | yes |
| `source_tables` | `refactor context` output | yes |
| `test_spec` | `test-specs/<table_fqn>.json` | yes (used by sandbox comparison) |

Assembled via:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context --table <table_fqn>
```

## Object Types: Proc Path vs View Path

The skill handles two object types determined by `context.object_type`:

| `object_type` | Path | Ground truth input |
|---|---|---|
| `table` | Proc path | `proc_body` + resolved `statements` (action=migrate) |
| `view` / `mv` | View path | `view_sql` (original view SQL body) |

**View path differences:**

- There is no `writer`, `proc_body`, or `statements` — these fields are absent.
- Sub-agent A receives `view_sql` directly as ground truth instead of extracting from a procedure body.
- Write-back via `refactor write` auto-detects the view and writes to the view catalog.
- The equivalence audit (`compare-sql`) is unchanged: `sql_a` = original view SQL, `sql_b` = refactored CTE SQL.

## Guards

Run before any execution:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor
```

Guard requires: scoping resolved, profile complete, test-spec present.

## Execution: Two Isolated Sub-agents

Sub-agents A and B run in **parallel** and must not see each other's output. This prevents context pollution so the equivalence comparison is meaningful. Both agents use `references/sp-migration-ref.md` for extraction and restructuring rules.

### Sub-agent A: Extract core SELECT (ground truth)

**Proc path:** Extract the core transformation SELECT from the procedure body by identifying the DML pattern and applying the extraction rules in `sp-migration-ref.md`. Produces a single pure T-SQL SELECT that returns the rows the procedure would write.

**View path:** Use `view_sql` directly — no extraction needed.

Output written to `.staging/<table_fqn>-extracted.sql`.

### Sub-agent B: Refactor into CTEs

Restructure the source SQL into the import/logical/final CTE pattern:

- **Import CTEs:** one per source table — `SELECT *` (or needed columns) from the bracket-quoted source table name. Named after the source.
- **Logical CTEs:** one transformation step per CTE (join, filter, aggregate, transform). Each does one thing.
- **Final CTE:** assembles the complete column list matching the target table.
- Ends with `SELECT * FROM final`.

If existing dbt staging or mart models are found in `dbt/models/`, Sub-agent B aligns import CTE column names and final CTE column ordering with those models.

Output written to `.staging/<table_fqn>-refactored.sql`.

## Equivalence Audit

After both sub-agents complete, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/<table_fqn>-extracted.sql \
  --sql-b-file .staging/<table_fqn>-refactored.sql \
  --spec test-specs/<table_fqn>.json
```

The CLI seeds fixtures from the test spec, executes both SELECTs in the sandbox, and returns per-scenario equivalence results (`a_minus_b`, `b_minus_a`).

### Self-correction loop (max 3 iterations)

If any scenario fails:

1. Analyse the diff — missing join, wrong filter, dropped column, type mismatch.
2. Revise **only** the refactored CTE SQL (sub-agent B's output). Sub-agent A's extracted SQL is the ground truth — never modify it.
3. Re-run `compare-sql`.

After 3 failed iterations, set `status = partial` and report remaining diffs.

## Output: `refactored_sql` Artifact

Written to the catalog via:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql \
  --status ok|partial|error
```

### Artifact format

The `refactored_sql` field in the catalog stores the CTE-structured SQL with this shape:

```sql
WITH import_<source1> AS (
    SELECT * FROM [schema].[SourceTable1]
),
import_<source2> AS (
    SELECT * FROM [schema].[SourceTable2]
),
<logical_cte_name> AS (
    -- one transformation step
    SELECT ...
    FROM import_<source1>
    JOIN import_<source2> ON ...
),
final AS (
    SELECT
        col1,
        col2,
        ...
    FROM <logical_cte_name>
)
SELECT * FROM final
```

- SQL stays in T-SQL (ISNULL, CONVERT, bracket quoting).
- Procedure parameters are replaced with literal defaults.
- Nested subqueries are flattened into sequential CTEs.
- Cursor loops become set-based operations (window functions, JOINs).
- Temp tables become logical CTEs.

### Status values

| `status` | Meaning |
|---|---|
| `ok` | All equivalence scenarios passed |
| `partial` | Max iterations reached; some scenarios still differ |
| `error` | Refactoring could not proceed at all |

## Output consumed by downstream skill

`generating-model` reads `refactored_sql` from the catalog as its primary input. The CTE structure directly maps to dbt model layers: import CTEs become ephemeral staging models (`stg_*`); logical and final CTEs become the mart model.

## Boundary Rules

The refactoring skill must not:

- Convert T-SQL to dbt Jinja or another dialect — that is `generating-model`'s job
- Modify sub-agent A's extracted SQL during self-correction
- Make profiling or materialization decisions
- Write dbt model files
