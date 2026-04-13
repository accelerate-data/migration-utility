---
name: profiling-table
description: >
  Use when profiling a single table, view, or materialized view for migration and the next step depends on persisted classification, keying, watermark, foreign-key typing, PII handling, or stg-vs-mart view classification.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Profiling Table

Persist a fresh profile for one table, view, or materialized view. Treat any existing `profile` section as non-authoritative and recompute from current catalog evidence.

## Arguments

`$ARGUMENTS` must be the fully-qualified object name. Ask the user if it is missing.

## Quick Flow

1. Check readiness:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready profile --object <fqn>
   ```

   If `ready` is `false`, report the failing `code` and `reason` and stop.
2. Detect object type:
   - `catalog/views/<fqn>.json` exists → use the **View Pipeline**
   - otherwise → use the **Table Pipeline**
3. Build the profile from current evidence, write it with `profile write`, then report the persisted result.

## Canonical Contracts

- Use the canonical `/profile` statuses and codes in [`../../lib/shared/profile_error_codes.md`](../../lib/shared/profile_error_codes.md).
- Diagnostics in `warnings` or `errors` must include `code`, `severity`, and `message`.
- If `profile write` rejects the payload, fix the JSON and retry.
- Do not set `status` yourself. `profile write` derives it.

## View Pipeline

### 1. Assemble Context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile view-context \
  --view <view_fqn>
```

If the command exits non-zero, stop and report the error.

### 2. Classify `stg` vs `mart`

Read the context JSON and apply [references/view-classification-signals.md](references/view-classification-signals.md).

Use this order:

1. Check `references.views.in_scope`. For each dependency view, run:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show --name <dependency_view_fqn>
   ```

   If any dependency view is already classified `mart`, inherit `mart`.
2. Use `sql_elements` next.
   - Aggregation, `group_by`, or `window_function` signals push to `mart`.
   - Single-source pass-through logic with no aggregation points to `stg`.
3. Use `logic_summary` only as a tiebreaker when structural signals are sparse.
4. For materialized views, aggregation still implies `mart`; lookup/pass-through still implies `stg`.
5. If signals conflict, default to `mart`.

Write a 1-2 sentence rationale naming the signals that drove the decision.

If view parsing continued with limitations, carry those diagnostics into `warnings` as canonical `/profile` entries. Normalize continued parse-limit warnings to `DDL_PARSE_ERROR` and preserve the original detail in `message`.

If classification is still ambiguous after dependency checks, `sql_elements`, and `logic_summary`, do not guess. Report the ambiguity and stop.

### 3. Write and Present

Write the profile JSON to a temp file, then persist it:

```bash
mkdir -p .staging
# Write profile JSON to .staging/view_profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <view_fqn> \
  --profile-file .staging/view_profile.json && rm -rf .staging
```

Required payload fields:

- `classification`: `stg` or `mart`
- `rationale`
- `source`: `llm`

After a successful write, report:

- classification
- rationale
- dependency views inspected, if any
- confirmation that the catalog was updated

## Table Pipeline

### 1. Assemble Context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile context \
  --table <table>
```

If the command exits non-zero, stop and report the error.

If `writer_ddl_slice` is present, the writer is a multi-table procedure. Use `writer_ddl_slice` as the primary SQL for this table. Use the full `proc_body` only for supporting context.

### 2. Answer the Six Profiling Questions

Read the context JSON and apply [references/profiling-signals.md](references/profiling-signals.md).

Answer the six profiling questions defined there. If the signals tentatively indicate:

- `fact_accumulating_snapshot` → also read [references/accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md)
- `fact_periodic_snapshot` → also read [references/periodic-snapshot-classification.md](references/periodic-snapshot-classification.md)

Follow the signal tables and pattern-matching rules in those references completely. Do not abbreviate them or replace them with a lighter heuristic.

### 3. Confidence Rules

- Do not guess. If a section cannot be supported confidently, omit that section from the payload.
- Separate writer opacity from table-shape ambiguity. If procedural analysis is incomplete but catalog signals and visible table shape still support a defensible classification, continue with a best-effort profile.
- Best-effort partial cases must use canonical warnings, not narrative substitutes:
  - unresolved sections but defensible classification → add `PARTIAL_PROFILE` with `severity: "warning"`
  - parse limits, dynamic SQL, or opaque helper `EXEC` also reducing confidence → also add `PARSE_ERROR` with `severity: "warning"`
- If `profile context` surfaces parse or routing diagnostics, copy the relevant detail into `warnings`.
- If you cannot support a defensible table classification at all, add `PROFILING_FAILED`, explain why, and stop instead of writing guessed output.
- Dynamic SQL and opaque helper cases are still write-through cases when table shape and catalog evidence support classification. Do not downgrade them to analysis-only output.

Required partial-friendly warning behavior:

- Opaque writer but defensible classification from catalog and table shape → include `PARTIAL_PROFILE`
- Opaque writer because of parse limits, dynamic SQL, or helper `EXEC` routing → include both `PARTIAL_PROFILE` and `PARSE_ERROR`
- For both codes above, `severity` must be exactly `"warning"`
- Dynamic SQL counts as partial even when classification, keys, and watermark can still be inferred confidently from catalog and table shape.

### 4. Write the Table Profile

Write the profile JSON to a temp file, then persist it:

```bash
mkdir -p .staging
# Write profile JSON to .staging/profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <table> \
  --profile-file .staging/profile.json && rm -rf .staging
```

Payload rules:

- `writer` is required.
- Every persisted decision must include a short `rationale`, including `classification`, `primary_key`, `natural_key`, `watermark`, each `foreign_keys[]` entry, and each `pii_actions[]` entry you emit.
- Use this payload shape:

  ```json
  {
    "writer": "<writer fqn>",
    "classification": {"resolved_kind": "...", "source": "...", "rationale": "..."},
    "primary_key": {"columns": ["..."], "primary_key_type": "...", "source": "...", "rationale": "..."},
    "natural_key": {"columns": ["..."], "source": "...", "rationale": "..."},
    "watermark": {"column": "...", "source": "...", "rationale": "..."},
    "foreign_keys": [{"column": "...", "fk_type": "...", "source": "...", "rationale": "..."}],
    "pii_actions": [{"column": "...", "suggested_action": "...", "source": "...", "rationale": "..."}],
    "warnings": [{"code": "...", "severity": "...", "message": "..."}],
    "errors": [{"code": "...", "severity": "...", "message": "..."}]
  }
  ```

- Omit unresolved sections entirely. Do not emit `null` placeholders.
- `foreign_keys[]` entries are per local FK column and use this shape: `{"column": "<local column>", "fk_type": "...", "source": "...", "rationale": "..."}`.
- Do not replace `column` with `columns`. Do not emit `reference` or nested `references` objects in the profile payload.
- Include every section you can support; omit unresolved sections instead of inventing values.
- Use canonical `warnings` and `errors` to explain omissions or reduced confidence.
- If `profile write` fails validation, correct the payload shape and retry before presenting results.
- Do not set `status`. Do not stop after reasoning. Persist the profile first, then summarize the persisted result.

After a successful write, report:

- classification with rationale
- primary key with source
- foreign keys with types
- natural key determination
- watermark column
- PII actions
- confirmation that the catalog was updated

## References

- [references/profiling-signals.md](references/profiling-signals.md) — six profiling questions and signal tables
- [references/accumulating-snapshot-classification.md](references/accumulating-snapshot-classification.md) — accumulating-snapshot disambiguation
- [references/periodic-snapshot-classification.md](references/periodic-snapshot-classification.md) — periodic-snapshot disambiguation
- [references/view-classification-signals.md](references/view-classification-signals.md) — view `stg` vs `mart` rules
- [`../../lib/shared/profile_error_codes.md`](../../lib/shared/profile_error_codes.md) — canonical `/profile` statuses and codes
