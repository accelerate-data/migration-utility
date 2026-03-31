---
name: profile
description: >
  Profile a single table for migration. Assembles deterministic context from catalog and DDL, reasons over the six profiling questions using what-to-profile-and-why.md, presents profile candidates for user approval, then writes the approved profile into the table catalog file. Use when the user asks to "profile a table", "classify a table", "what kind of model is this table", or wants to determine PK, FK, watermark, or PII for a migration target.
user-invocable: true
argument-hint: "--table <table> --writer <writer>"
---

# Profile

Profile a single table for migration by assembling context, reasoning over six profiling questions, and writing results to the table catalog file.

## Arguments

Parse `$ARGUMENTS` for `--table` and `--writer`. Use `AskUserQuestion` if either is missing.

## Before invoking any subcommand

Read `manifest.json` from the current working directory to confirm a valid project root. If missing, stop and tell the user to run `setup-ddl` first.

## Pipeline

### Step 1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile context \
  --table <table> --writer <writer>
```

This reads catalog signals, writer references, proc body, column list, and related procedure context. Output is a JSON matching `lib/shared/schemas/profile_context.json`.

If exit code is non-zero, stop and report the error.

### Step 2 -- LLM Profiling (Reasoning)

Read the context JSON and the reference tables from `docs/design/agent-contract/what-to-profile-and-why.md`. Answer the six profiling questions below.

**Key principle:** Catalog signals are facts, not candidates. If the catalog declares a PK, that is the PK (`source: "catalog"`). If the catalog has declared FKs, those are confirmed FKs. The LLM fills in what the catalog does not answer (`source: "llm"`). When the LLM adds detail to a catalog fact (e.g. classifying `fk_type` on a declared FK), use `source: "catalog+llm"`.

#### Q1 -- Classification (`resolved_kind`)

Read the writer proc body. Match write patterns against the classification table in `what-to-profile-and-why.md`:

- Identify the dominant DML pattern (INSERT-only, MERGE, TRUNCATE+INSERT, etc.)
- Check column shape signals (SCD columns, milestone dates, snapshot dates, flag columns)
- Use `writer_references` to confirm which tables are read vs written
- Produce `resolved_kind` from the 8 allowed values: `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate`

#### Q2 -- Primary Key

Check catalog `primary_keys` first. If declared, that is the answer.

If no declared PK:

- Look for MERGE ON clause in proc body
- Look for UPDATE/DELETE WHERE col = @param
- Use `auto_increment_columns` as surrogate signal

#### Q3 -- Foreign Keys

Check catalog `foreign_keys` first. If declared, those are confirmed. Classify each `fk_type` (standard/role_playing/degenerate) using proc JOIN patterns.

If no declared FKs:

- Use `writer_references` column-level `is_selected` flags
- Use `referenced_by` to find reader procs joining on the same column
- Apply naming-convention patterns (`_sk`/`_id` suffix)

#### Q4 -- Natural Key vs Surrogate Key

Check catalog `auto_increment_columns`. If present, the PK is surrogate.

If no identity column:

- Look for `NEWID()` / `NEWSEQUENTIALID()` / `NEXT VALUE FOR` in proc body
- Check column name patterns (`_sk`/`_guid` = surrogate; `_code`/`_number` = natural)
- MERGE ON using different column from INSERT PK = classic surrogate pattern

#### Q5 -- Watermark

- Look for WHERE clause filtering in proc body (`WHERE col > @last_run`, `BETWEEN @start AND @end`)
- Check column name patterns (`modified_at`, `load_date`, `_dt`, `_ts`)
- Use `change_capture` from catalog to inform strategy

#### Q6 -- PII Actions

Check catalog `sensitivity_classifications` first. If populated, those are confirmed.

For remaining columns:

- Match column names against PII patterns (email, ssn, phone, address, etc.)
- Assign suggested action: `mask` (default), `drop`, `tokenize`, `keep`

### Step 3 -- Present for Approval

Present the profile as a structured summary for user review. Include:

- Classification with rationale
- Primary key with source
- Foreign keys with types
- Natural key vs surrogate key determination
- Watermark column
- PII actions

**Stop on ambiguity.** If a required question (Q1, Q2, Q4, Q5) cannot be answered with reasonable confidence, present the ambiguity to the user and ask for guidance. Do not auto-resolve unclear classifications.

Wait for explicit user approval before proceeding to Step 4.

### Step 4 -- Write to Catalog (Deterministic)

After user approval (with any edits):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile write \
  --table <table> \
  --profile '<json>'
```

The profile JSON must match the `profile_section` schema in `lib/shared/schemas/table_catalog.json`. Required fields: `status`, `writer`. All enum values must be from the allowed sets.

If the write exits non-zero, report the validation errors and ask the user to correct.

## Output Schema

The `profile` section written to `catalog/tables/<table>.json` follows `table_catalog.json#/$defs/profile_section`.

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Domain/validation failure (missing catalog, invalid enum) |
| 2 | IO or parse error |
