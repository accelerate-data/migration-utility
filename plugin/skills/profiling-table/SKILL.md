---
name: profiling-table
description: >
  Use when profiling a single table, view, or materialized view for migration and the next step depends on persisted classification, keying, watermark, foreign-key typing, PII handling, or stg-vs-mart view classification.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Profiling Table

Profile a single table, view, or materialized view for migration.

## Arguments

`$ARGUMENTS` is the fully-qualified name. Ask the user if missing.

## Schema discipline

Use the canonical `/profile` surfaced code list in `../../lib/shared/profile_error_codes.md`. If `profile write` returns a validation error, fix the JSON and retry.

Diagnostics written to `warnings` or `errors` must use canonical `/profile` entries from that file. Include at least:

- `code`
- `severity`
- `message`

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <fqn> profile
```

If `ready` is `false`, report the failing `code` and `reason` to the user and stop. If `code` is absent, report the `reason`.

## Object type detection

Check whether `catalog/views/<fqn>.json` exists:

- **If yes** → this is a **view or MV**. Follow the **View Profile Pipeline** below.
- **If no** → this is a **table**. Follow the **Table Profile Pipeline** below.

---

## View Profile Pipeline

### Step V1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile view-context \
  --view <view_fqn>
```

Output shape:

```json
{
  "view": "<normalized FQN>",
  "is_materialized_view": bool,
  "sql_elements": [{"type": "join|aggregation|window_function|case|subquery|cte|group_by", "detail": "..."}] | null,
  "logic_summary": "<plain-language description>" | null,
  "columns": [{"name": str, "sql_type": str}],  // MV only
  "references": {"tables|views|functions": {"in_scope": [{"schema": str, "name": str, "object_type": str}], "out_of_scope": [...]}},
  "referenced_by": {"procedures|views|functions": {"in_scope": [{"schema": str, "name": str, "object_type": str}], "out_of_scope": [...]}},
  "warnings": [], "errors": []
}
```

Key fields:

- `references` — outbound refs (tables, views, functions) with `object_type` on each in_scope entry
- `referenced_by` — inbound refs (procedures, views, functions) with `object_type` on each in_scope entry

If exit code is non-zero, stop and report the error.

### Step V2 -- LLM Classification (Reasoning)

Read the context JSON and apply the signal table in [view-classification-signals.md](references/view-classification-signals.md).

Answer one question: **Is this view `stg` or `mart`?**

Steps:

1. Check `references.views.in_scope`. For each dependency view, run:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <dependency_view_fqn>
   ```

   Inspect `profile.classification`. If any dependency is `mart`, inherit `mart`.
2. Apply the signal table to `sql_elements`. Aggregation, group_by, window_function → `mart`. Single-source with no aggregation → `stg`.
3. Use `logic_summary` as tiebreaker when `sql_elements` is empty or null.
4. For materialized views: aggregation signals → `mart`; lookup/pass-through → `stg`.
5. When signals conflict: default to `mart`.

Write a 1–2 sentence rationale citing the specific signals that drove the decision.

If the view context carries parse-limit diagnostics, preserve them in the profile payload as canonical `/profile` warnings. Normalize any continued parse-limit entry to `DDL_PARSE_ERROR` with `severity: "warning"` and keep the original detail in `message`.

### Step V3 -- Write to Catalog (Deterministic)

Persist the view profile. Write the profile JSON to a temp file:

```bash
mkdir -p .staging
# Write profile JSON to .staging/view_profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <view_fqn> \
  --profile-file .staging/view_profile.json && rm -rf .staging
```

Do not include `status` in the profile JSON.

Required fields: `classification`, `rationale`, `source`.
Optional fields: `warnings`, `errors`.

| Field | Valid values |
|---|---|
| `classification` | `stg`, `mart` |
| `source` | `llm` |

If classification cannot be supported from dependency signals, `sql_elements`, or `logic_summary`, do not guess. Report the ambiguity to the user and stop instead of forcing a write.

If the write exits non-zero, report the validation errors and retry with corrected JSON.

### Step V4 -- Present Persisted Result

After `profile write` succeeds, present the classification summary:

- Classification (`stg` or `mart`)
- Rationale (which signals drove the decision)
- Any dependency views inspected and their classifications
- Confirmation that the profile was written to the catalog

### View References

- [references/view-classification-signals.md](references/view-classification-signals.md) — signal table and tie-breaking rules for stg vs mart classification

---

## Table Profile Pipeline

### Step 1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile context \
  --table <table>
```

Output shape:

```json
{
  "table": "<normalized FQN>",
  "writer": "<writer procedure FQN>",
  "catalog_signals": {"primary_keys": [...], "foreign_keys": [...], "auto_increment_columns": [...], "unique_indexes": [...], "change_capture": {...} | null, "sensitivity_classifications": [...]},
  "writer_references": {"tables|views|functions|procedures": {"in_scope": [...], "out_of_scope": [...]}},
  "proc_body": "<full SQL body>",
  "columns": [{"name": str, "sql_type": str, "is_nullable": bool, "is_identity": bool}],
  "related_procedures": [{"procedure": "<FQN>", "proc_body": "<SQL>", "references": {...}}],
  "writer_ddl_slice": "<DDL slice for target table>" | null
}
```

If exit code is non-zero, stop and report the error.

**Multi-table-writer:** If `writer_ddl_slice` is present in the context, the writer is a multi-table proc. Focus your analysis on `writer_ddl_slice` as the primary SQL — it contains only the portion of the proc relevant to this table. The full `proc_body` is provided for reference only and may contain logic for other tables.

### Step 2 -- LLM Profiling (Reasoning)

Read the context JSON and the signal tables in [profiling-signals.md](references/profiling-signals.md). Answer the six profiling questions (Q1–Q6) defined there. Follow all signal tables and pattern matching rules — do not abbreviate. If any signal tentatively points to `fact_accumulating_snapshot`, also read [accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md) and apply its decision guide before confirming. If any signal tentatively points to `fact_periodic_snapshot`, also read [periodic-snapshot-classification.md](references/periodic-snapshot-classification.md) and apply its decision guide before confirming.

Confidence rules:

- Do not guess. If a question cannot be answered confidently, omit that section from the profile payload rather than inventing a value.
- Treat writer-body opacity separately from table-shape ambiguity. If static analysis is incomplete because of dynamic SQL, cross-database helpers, or other parse limits, but catalog signals and visible table shape still support a defensible classification, continue with a best-effort classification instead of escalating to `PROFILING_FAILED`.
- Typical best-effort cases that should still be written as `partial` include:
  - a statically identified writer that delegates with `EXEC` to an opaque helper
  - dynamic SQL where the table shape and catalog signals still clearly indicate a simple dimension or fact pattern
- When one or more profiling questions remain unresolved but best-effort profiling can continue, add a canonical warning entry with `code: "PARTIAL_PROFILE"` and `severity: "warning"`.
- When procedure-side parse limitations materially reduce confidence but profiling can still continue, add a canonical warning entry with `code: "PARSE_ERROR"` and `severity: "warning"`, preserving the raw parse detail in `message`. If `profile context` surfaces parse or routing diagnostics, copy the relevant detail into `warnings`; do not leave it only in the reasoning narrative.
- If profiling cannot support a defensible table classification at all, add a canonical error entry with `code: "PROFILING_FAILED"` and `severity: "error"`, explain why to the user, and stop instead of writing a guessed classification.

Required warning behavior for partial-friendly cases:

- Opaque writer but defensible classification from catalog + table shape → include `PARTIAL_PROFILE`.
- Opaque writer because of parse limits, dynamic SQL, or helper `EXEC` routing → include both `PARTIAL_PROFILE` and `PARSE_ERROR`.
- Do not substitute narrative text for these warning entries. If the case is partial, the warning codes must be present in the payload.

### Step 3 -- Write to Catalog (Deterministic)

Persist the table profile. Write the profile JSON to a temp file:

```bash
mkdir -p .staging
# Write profile JSON to .staging/profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <table> \
  --profile-file .staging/profile.json && rm -rf .staging
```

Do not include `status` in the profile JSON.

Required fields: `writer`. Each decision point must include a `rationale` field (1–2 sentences): `classification.rationale`, `primary_key.rationale`, `natural_key.rationale`, `watermark.rationale`, and per-entry `rationale` in `foreign_keys[]` and `pii_actions[]`.
Optional fields: `warnings`, `errors`.

Status is derived by `profile write`; do not set it yourself. Best-effort payload rules:

- Include every section you can support from the signals.
- Omit unresolved sections instead of filling them with guesses.
- Use canonical `warnings`/`errors` entries to explain why any section is omitted.
- If you continue despite parser or routing limits, include `warnings`. A best-effort partial write should normally carry `PARTIAL_PROFILE`, and parse-limited cases should also carry `PARSE_ERROR`.
- When classification comes mainly from catalog and table-shape evidence because the writer body is opaque, still write `classification` with a rationale that names the missing procedural evidence and why the remaining signals are sufficient.
- A payload with `classification` but no `primary_key` will persist as `partial`; a payload with no supported `classification` will persist as `error`.

Examples for partial-friendly writes:

Cross-database helper or opaque `EXEC`:

```json
{
  "writer": "silver.usp_load_DimCrossDbProfile",
  "classification": {
    "resolved_kind": "dim_non_scd",
    "source": "catalog+llm",
    "rationale": "Catalog signals and table shape support a simple dimension classification even though the helper proc body is opaque."
  },
  "primary_key": {
    "columns": ["CrossDbProfileKey"],
    "primary_key_type": "surrogate",
    "source": "catalog",
    "rationale": "Identity PK in catalog."
  },
  "warnings": [
    {
      "code": "PARTIAL_PROFILE",
      "severity": "warning",
      "message": "Writer delegates to an opaque helper, so some profiling decisions remain best-effort."
    },
    {
      "code": "PARSE_ERROR",
      "severity": "warning",
      "message": "Cross-database helper body is unavailable for static analysis."
    }
  ]
}
```

Dynamic SQL with defensible table classification:

```json
{
  "writer": "silver.usp_load_DimCurrency",
  "classification": {
    "resolved_kind": "dim_non_scd",
    "source": "catalog+llm",
    "rationale": "Table shape and visible load pattern support a simple dimension classification despite dynamic SQL."
  },
  "warnings": [
    {
      "code": "PARTIAL_PROFILE",
      "severity": "warning",
      "message": "Dynamic SQL prevents a complete static read of the writer."
    },
    {
      "code": "PARSE_ERROR",
      "severity": "warning",
      "message": "Carry forward the parse limitation surfaced by profile context."
    }
  ]
}
```

All enum values must be from the allowed sets below:

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

If the write exits non-zero, report the validation errors and retry with corrected JSON.

### Step 4 -- Present Persisted Result

After `profile write` succeeds, present the profile as a structured summary. Include:

- Classification with rationale
- Primary key with source
- Foreign keys with types
- Natural key vs surrogate key determination
- Watermark column
- PII actions
- Confirmation that the profile was written to the catalog

## References

- [references/profiling-signals.md](references/profiling-signals.md) — six profiling questions (Q1–Q6), signal tables, and pattern matching rules for classification, keys, watermark, and PII
- [references/accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md) — decision guide for confirming `fact_accumulating_snapshot`: primary write-pattern signals, negative signals, and role-playing FK date key disambiguation
- [references/periodic-snapshot-classification.md](references/periodic-snapshot-classification.md) — decision guide for confirming `fact_periodic_snapshot`: snapshot date vs. role-playing FK date keys, semi-additive vs. fully additive measures, calendar join and GROUP BY grain signals
- [`../../lib/shared/profile_error_codes.md`](../../lib/shared/profile_error_codes.md) — canonical `/profile` statuses and surfaced error/warning codes

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `profile context` | 1 | Catalog file missing for table or writer. Report which prerequisite is missing |
| `profile context` | 2 | Writer unresolved, DDL parse failed, or catalog/load error. Surface the error message and stop |
| `profile view-context` | 1 | View catalog missing or view scoping not completed. Report the prerequisite failure and stop |
| `profile view-context` | 2 | Catalog/load error. Surface the error message and stop |
| `profile write` | 1 | Validation failure or missing catalog file. Report the error, fix the payload if possible, and retry |
| `profile write` | 2 | Invalid JSON payload, catalog load failure, or write IO failure. If the JSON is malformed, fix it and retry; otherwise report the error and stop |
