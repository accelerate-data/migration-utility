---
name: profiling-table
description: >
  Profile a single table for migration. Assembles deterministic context from catalog and DDL, reasons over the six profiling questions using what-to-profile-and-why.md, presents profile candidates for user approval, then writes the approved profile into the table catalog file. Use when the user asks to "profile a table", "classify a table", "what kind of model is this table", or wants to determine PK, FK, watermark, or PII for a migration target.
user-invocable: true
argument-hint: "<schema.table>"
---

# Profile

Profile a single table for migration by assembling context, reasoning over six profiling questions, and writing results to the table catalog file.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Use `AskUserQuestion` if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` → `scoping.selected_writer`).

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.
2. Confirm `catalog/tables/<table>.json` exists. If missing, tell the user to run `/listing-objects list tables` to see available tables and stop.

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

After user approval (with any edits):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" profile write \
  --table <table> \
  --profile '<json>'
```

The profile JSON must match the `profile_section` schema in `lib/shared/schemas/table_catalog.json`. Required fields: `status`, `writer`. All enum values must be from the allowed sets. Each decision point must include a `rationale` field (1–2 sentences): `classification.rationale`, `primary_key.rationale`, `natural_key.rationale`, `watermark.rationale`, and per-entry `rationale` in `foreign_keys[]` and `pii_actions[]`.

If the write exits non-zero, report the validation errors and ask the user to correct.

## Output Schema

The `profile` section written to `catalog/tables/<table>.json` follows `table_catalog.json#/$defs/profile_section`.

## Exit Codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Domain/validation failure (missing catalog, invalid enum) |
| 2 | IO or parse error |
