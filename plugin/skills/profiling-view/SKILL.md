---
name: profiling-view
description: >
  Profile a single view or materialized view for migration. Assembles context from the view catalog, classifies it as stg or mart using sql_elements, logic_summary, and dependency signals, presents the classification for user approval, then writes the result to the view catalog. Use when the user asks to "profile a view", "classify a view", or wants to determine whether a view is staging or a reporting mart.
user-invocable: true
argument-hint: "<schema.view>"
---

# Profiling View

Profile a single view or materialized view by assembling context, classifying it as `stg` or `mart`, and writing the result to the view catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified view name. Ask the user if missing.

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <view_fqn> profile-view
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Pipeline

### Step 1 -- Assemble Context (Deterministic)

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile view-context \
  --view <view_fqn>
```

Output is a JSON matching `lib/shared/schemas/view_profile_context.json`. It contains:

- `view` — normalized FQN
- `is_materialized_view` — true for Oracle MVs and SQL Server indexed views
- `sql_elements` — SQLglot-extracted SQL features (join, aggregation, window_function, case, subquery, cte, group_by); null if DDL parse failed
- `logic_summary` — plain-language description of what the view computes
- `columns` — present only for materialized views
- `references` — outbound refs (tables, views, functions) with `object_type` on each in_scope entry
- `referenced_by` — inbound refs (procedures, views, functions) with `object_type` on each in_scope entry

If exit code is non-zero, stop and report the error.

### Step 2 -- LLM Classification (Reasoning)

Read the context JSON and apply the signal table in [view-classification-signals.md](references/view-classification-signals.md).

Answer one question: **Is this view `stg` or `mart`?**

Steps:

1. Check `references.views.in_scope`. For each, call `discover show --name <fqn>` to inspect its `profile.classification`. If any dependency is `mart`, inherit `mart`.
2. Apply the signal table to `sql_elements`. Aggregation, group_by, window_function → `mart`. Single-source with no aggregation → `stg`.
3. Use `logic_summary` as tiebreaker when `sql_elements` is empty or null.
4. For materialized views: aggregation signals → `mart`; lookup/pass-through → `stg`.
5. When signals conflict: default to `mart`.

Write a 1–2 sentence rationale citing the specific signals that drove the decision.

### Step 3 -- Present for Approval

Present the classification summary:

- Classification (`stg` or `mart`)
- Rationale (which signals drove the decision)
- Any dependency views inspected and their classifications

**Stop on ambiguity.** If the classification cannot be determined with reasonable confidence, present the ambiguity and ask for guidance.

Wait for explicit user approval before proceeding to Step 4.

### Step 4 -- Write to Catalog (Deterministic)

After user approval (with any edits), write the profile JSON to a temp file:

```bash
mkdir -p .staging
# Write profile JSON to .staging/view_profile.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" profile write \
  --table <view_fqn> \
  --profile-file .staging/view_profile.json; rm -rf .staging
```

The profile JSON must match the `profile` section in `lib/shared/schemas/view_catalog.json`. Required fields: `status`, `classification`, `rationale`, `source`.

| Field | Valid values |
|---|---|
| `status` | `ok`, `partial`, `error` |
| `classification` | `stg`, `mart` |
| `source` | `llm` |

If the write exits non-zero, report the validation errors and ask the user to correct.

## Output Schema

The `profile` section written to `catalog/views/<view>.json` follows the `profile` property in `lib/shared/schemas/view_catalog.json`.

## References

- [references/view-classification-signals.md](references/view-classification-signals.md) — signal table and tie-breaking rules for stg vs mart classification

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate-util guard` | non-zero | Report failing guard code and message; stop |
| `profile view-context` | 1 | Catalog file missing or scoping not completed. Report which prerequisite is missing |
| `profile view-context` | 2 | IO/parse error. Surface the error message |
| `profile write` | 1 | Validation failure (invalid JSON, missing fields, bad enums). Report errors, ask user to correct |
| `profile write` | 2 | IO error (catalog unreadable, write failure). Report and stop |
