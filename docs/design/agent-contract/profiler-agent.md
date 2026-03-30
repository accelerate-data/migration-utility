# Profiler Agent Contract

The profiler agent produces migration profile candidates for each table in a batch. It runs `profile.py` for context assembly, applies LLM reasoning to answer the six profiling questions, and writes results into each table's catalog file.

For the interactive single-table path, see the `/profile` skill in [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md). Both paths share `profile.py` for deterministic context assembly; the LLM reasoning is replicated with context-appropriate prompting (batch: no approval gates, structured output, skip-and-continue on errors; interactive: present for approval, stop on ambiguity).

## Philosophy and Boundary

- All catalog signals are pre-captured by setup-ddl (VU-766) in `catalog/` files. The agent never queries the live database.
- Proc bodies are in DDL files from setup-ddl. No `sys.sql_modules` access needed.
- `profile.py` (shared) has two subcommands: `context` (assemble LLM input) and `write` (merge profile into catalog file). The agent does LLM reasoning between the two.
- FDE approves profile results before the migrator consumes them.
- No sampled data profiling — live DB is unavailable at migration time.

## Required Input

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "project_root": "/path/to/artifacts",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "selected_writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```

No `related_procedure_depth` — related procedure context is pre-captured in catalog files.

## Agent Pipeline

For each item in `items[]`:

### 1. AssembleContext (Deterministic — `profile.py context`)

Run `uv run profile context --table <item_id> --writer <selected_writer> --project-root <project_root> --dialect tsql`.

`profile.py context` reads:

- `catalog/tables/<table>.json` — catalog signals (PKs, FKs, identity, CDC, sensitivity) + `referenced_by` with `is_selected`/`is_updated` flags
- `catalog/procedures/<writer>.json` — `references` with column-level read/write flags
- `procedures.sql` — writer proc body
- `tables.sql` — column list
- Related procedure catalog files and bodies where referenced

Outputs a single context JSON to stdout. See [SP → dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for the full output schema.

### 2. ProfileWithLLM

Using the context JSON and the reference tables from [What to Profile and Why](what-to-profile-and-why.md), answer the six profiling questions.

**Catalog facts are answers, not candidates.** If the catalog declares a PK, that is the PK. If the catalog has declared FKs, those are confirmed FKs. The LLM fills in what the catalog doesn't answer.

#### Q1 — Classification

Read the writer proc body. Match write patterns against the classification table in `what-to-profile-and-why.md`:

- Identify the dominant DML pattern (INSERT-only, MERGE, TRUNCATE+INSERT, etc.)
- Check column shape signals (SCD columns, milestone dates, snapshot dates, flag columns)
- Use `writer_references` to confirm which tables are read vs written
- Produce `resolved_kind` from the allowed classification kinds

#### Q2 — Primary Key

Check catalog `primary_keys` first — if declared, that is the answer (`source: "catalog"`).

If no declared PK:

- Look for MERGE ON clause in proc body — strongest code-level signal
- Look for UPDATE/DELETE WHERE col = @param — single-row lookup key
- Use `auto_increment_columns` from catalog as surrogate signal

#### Q3 — Foreign Keys

Check catalog `foreign_keys` first — if declared, those are confirmed (`source: "catalog"`). Classify `fk_type` (standard/role_playing/degenerate) using proc JOIN patterns.

If no declared FKs:

- Use `writer_references` column-level `is_selected` flags to see which dimension tables the writer JOINs
- Use `referenced_by` to see which reader procs join on which columns — multiple independent readers joining on the same column is high confidence
- Apply naming-convention patterns (`_sk`/`_id` suffix → dimension table stem match)

#### Q4 — Natural Key vs Surrogate Key

Check catalog `auto_increment_columns` — if present, the PK is surrogate (`source: "catalog"`).

If no identity column:

- Look for `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body
- Check column name patterns (`_sk`/`_guid` → surrogate; `_code`/`_number` → natural)
- MERGE ON clause using different column from INSERT PK → classic surrogate pattern

#### Q5 — Watermark

- Look for WHERE clause filtering in proc body (`WHERE col > @last_run`, `BETWEEN @start AND @end`)
- Check column name patterns (`modified_at`, `load_date`, `_dt`, `_ts`)
- Use `change_capture` from catalog to inform strategy (does not identify the column)

#### Q6 — PII Actions

Check catalog `sensitivity_classifications` first — if populated, those are confirmed (`source: "catalog"`).

For remaining columns:

- Match column names against PII patterns (email, ssn, phone, address, etc.)
- Consider column type + context (VARCHAR/NVARCHAR with PII-suggestive names)
- Assign suggested action: `mask` (default), `drop`, `tokenize`, `keep`

### 3. WriteCatalogFile (Deterministic — `profile.py write`)

Run `uv run profile write --table <item_id> --project-root <project_root> --profile '<json>'`.

The `write` subcommand:

1. Reads existing `catalog/tables/<item_id>.json`
2. Validates the profile JSON structure (required fields, allowed enum values)
3. Merges the `profile` section into the catalog file
4. Writes back atomically
5. Outputs confirmation JSON to stdout

The agent passes the LLM-produced profile as a JSON string argument. Python handles all file I/O and validation — the agent never writes files directly.

Profile section schema:

```json
{
  "profile": {
    "status": "ok|partial|error",
    "writer": "dbo.usp_load_fact_sales",
    "classification": {
      "resolved_kind": "fact_transaction",
      "rationale": "Pure INSERT with no UPDATE or DELETE in writer proc.",
      "source": "llm"
    },
    "primary_key": {
      "columns": ["sale_id"],
      "primary_key_type": "surrogate",
      "source": "catalog"
    },
    "natural_key": {
      "columns": ["order_id", "line_number"],
      "rationale": "MERGE ON clause uses these columns as business key.",
      "source": "llm"
    },
    "watermark": {
      "column": "load_date",
      "rationale": "WHERE load_date > @last_run in writer proc.",
      "source": "llm"
    },
    "foreign_keys": [
      {
        "column": "customer_sk",
        "references_source_relation": "dbo.dim_customer",
        "references_column": "customer_sk",
        "fk_type": "standard",
        "source": "catalog+llm"
      }
    ],
    "pii_actions": [
      {
        "column": "customer_email",
        "entity": "email",
        "suggested_action": "mask",
        "source": "llm"
      }
    ],
    "warnings": [],
    "errors": []
  }
}
```

### 4. HandleErrors

- If `profile.py` fails: set `status: "error"`, record in `errors[]`, continue to next item.
- If LLM cannot answer a required question (classification, primary_key, watermark): set `status: "partial"`, record which questions are unresolved in `warnings[]`, continue to next item.
- Do not stop the batch on individual item failures.

## `source` Field Semantics

- `"catalog"` — fact from setup-ddl catalog data. Not inferred.
- `"llm"` — inferred by LLM from proc body / column patterns / reference tables.
- `"catalog+llm"` — catalog provided the base fact (e.g. declared FK), LLM added classification (e.g. `fk_type`).

## `status` Field

- `ok` — required questions answered (classification, primary_key, watermark).
- `partial` — one or more required questions unanswered.
- `error` — runtime failure prevented profiling.

`natural_key` may be empty and still `ok` when `primary_key_type == "surrogate"`.

## Batch Output

After processing all items, the agent assembles a summary:

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok",
      "catalog_path": "catalog/tables/dbo.fact_sales.json"
    }
  ],
  "summary": {
    "total": 1,
    "ok": 1,
    "partial": 0,
    "error": 0
  }
}
```

The actual profile data lives in the catalog file, not duplicated in the batch output.

## Classification Kinds

`profile.classification.resolved_kind` must be one of:

- `dim_non_scd`
- `dim_scd1`
- `dim_scd2`
- `dim_junk`
- `fact_transaction`
- `fact_periodic_snapshot`
- `fact_accumulating_snapshot`
- `fact_aggregate`

## Foreign Key Types

`profile.foreign_keys[*].fk_type` must be one of:

- `standard`
- `role_playing`
- `degenerate`

## Suggested PII Actions

`profile.pii_actions[*].suggested_action` must be one of:

- `mask`
- `drop`
- `tokenize`
- `keep`

## Namespace Rules

- `profile.foreign_keys[*].references_source_relation` and `references_column` are source-side SQL Server identifiers.
- Profiler must not emit dbt `ref()` names. Namespace translation is migrator scope.

## What Profiler Must Not Output

- Final dbt SQL or Jinja model content.
- Final materialization/test decisions (migrator's job).

`warnings[]` and `errors[]` use the shared diagnostics schema in `docs/design/agent-contract/README.md`.

## Handoff

- Decomposer consumes `item_id` and `selected_writer` from application-routed inputs.
- Planner consumes approved profile answers from `catalog/tables/<table>.json` → `profile` section.
