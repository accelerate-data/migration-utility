---
name: profiling-table
description: >
  Profile a single table for migration. Assembles deterministic context from catalog and DDL, reasons over the six profiling questions using what-to-profile-and-why.md, presents profile candidates for user approval, then writes the approved profile into the table catalog file. Use when the user asks to "profile a table", "classify a table", "what kind of model is this table", or wants to determine PK, FK, watermark, or PII for a migration target.
user-invocable: true
argument-hint: "<schema.table>"
---

# Profiling Table

Profile a single table for migration by assembling context, reasoning over six profiling questions, and writing results to the table catalog file.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` → `scoping.selected_writer`).

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, tell the user to run `/listing-objects list tables` to see available tables and stop.
3. Read `catalog/tables/<table>.json` and confirm `scoping.selected_writer` is set. If missing, tell the user to run `/scoping-table <table>` first and stop.

## Pipeline

### Step 1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile context \
  --table <table>
```

The CLI reads the selected writer from the table's catalog scoping section — no `--writer` argument needed.

This reads catalog signals, writer references, proc body, column list, and related procedure context. Output is a JSON matching `lib/shared/schemas/profile_context.json`.

If exit code is non-zero, stop and report the error.

### Step 2 -- LLM Profiling (Reasoning)

Read the context JSON and the signal tables in [profiling-signals.md](references/profiling-signals.md). Answer the six profiling questions (Q1–Q6) defined there. Follow all signal tables and pattern matching rules — do not abbreviate.

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

After user approval (with any edits), write the profile JSON to a temp file to avoid shell escaping issues:

```bash
mkdir -p .staging
# Write profile JSON to .staging/profile.json (avoids shell quoting breakage from apostrophes in rationale text)
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile write \
  --table <table> \
  --profile-file .staging/profile.json; rm -rf .staging
```

The profile JSON must match the `profile_section` schema in `lib/shared/schemas/table_catalog.json`. Required fields: `status`, `writer`. Each decision point must include a `rationale` field (1–2 sentences): `classification.rationale`, `primary_key.rationale`, `natural_key.rationale`, `watermark.rationale`, and per-entry `rationale` in `foreign_keys[]` and `pii_actions[]`.

All enum values must be from the allowed sets below (canonical source: `lib/shared/profile.py`):

| Field | Valid values |
|---|---|
| `status` | `ok`, `partial`, `error` |
| `classification.resolved_kind` | `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate` |
| `classification.source` | `catalog`, `llm`, `catalog+llm` |
| `primary_key.primary_key_type` | `surrogate`, `natural`, `composite`, `unknown` |
| `primary_key.source` | `catalog`, `llm`, `catalog+llm` |
| `natural_key.source` | `catalog`, `llm`, `catalog+llm` |
| `watermark.source` | `catalog`, `llm`, `catalog+llm` |
| `foreign_keys[*].fk_type` | `standard`, `role_playing`, `degenerate` |
| `foreign_keys[*].source` | `catalog`, `llm`, `catalog+llm` |
| `pii_actions[*].suggested_action` | `mask`, `drop`, `tokenize`, `keep` |
| `pii_actions[*].source` | `catalog`, `llm`, `catalog+llm` |

If the write exits non-zero, report the validation errors and ask the user to correct.

## Output Schema

The `profile` section written to `catalog/tables/<table>.json` follows `table_catalog.json#/$defs/profile_section`.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `profile context` | 1 | Catalog file missing for table or writer. Report which prerequisite is missing |
| `profile context` | 2 | IO/parse error. Surface the error message |
| `profile write` | 1 | Validation failure (invalid JSON, missing fields, bad enums). Report errors, ask user to correct |
| `profile write` | 2 | IO error (catalog unreadable, write failure). Report and stop |
