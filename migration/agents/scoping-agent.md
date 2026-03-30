---
name: scoping-agent
description: Identifies writer procedures from catalog data and produces a CandidateWriters JSON output. Use when scoping a migration item.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Scoping Agent

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable.

Use `uv run discover` directly for all analysis — do not invoke the discover skill. Do not read catalog files directly — use `discover refs` and `discover show` as your interface.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../lib/shared/schemas/scope_input.json`
- **Output schema:** `../lib/shared/schemas/candidate_writers.json`

After reading the input, read `<ddl_path>/manifest.json` for `technology` and `dialect`. If manifest is missing or unreadable, fail all items with code `MANIFEST_NOT_FOUND` and write output immediately.

---

## Pipeline

### Step 1 — Discover Refs

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../lib" discover refs \
  --ddl-path <ddl_path> --name <item_id>
```

The output contains `writers` and `readers`. Each writer is a candidate.

If discover fails (exit code 1 or 2), record `error` with code `DISCOVER_EXECUTION_FAILED`.

### Step 2 — Enrich Each Candidate

For each candidate writer:

1. Run `discover show --ddl-path <ddl_path> --name <writer>`.

2. Extract `refs` from the output. For every ref that is a view, function, or procedure (not a base table), run `discover show` on it and follow the chain until you reach base tables. Assemble the fully resolved `dependencies: { tables, views, functions }` on the candidate.

3. Check `classification`:
   - `deterministic` — no further action needed.
   - `claude_assisted` — read `raw_ddl` and analyse the proc body:
     - Identify `reads_from` and `writes_to`.
     - Classify each statement as `migrate` or `skip`. See `../skills/discover/references/tsql-parse-classification.md` for the full classification guide.
     - If the proc calls other procs (EXEC), run `discover show` on each and follow recursively.
     - Populate `llm_analysis` on the candidate with `reads_from`, `writes_to`, `statements`, and `rationale`.
     - Add `LLM_ANALYSIS_REQUIRED` warning.

The `llm_analysis` field preserves your analysis so downstream agents don't re-read the same proc.

### Step 3 — Apply Resolution Rules

| Condition | Status |
|---|---|
| One writer | `resolved` — set `selected_writer` |
| Two or more writers | `ambiguous_multi_writer` |
| No writers | `no_writer_found` |
| Discover failed | `error` |

### Step 4 — Validate and Write Output

Non-obvious cross-field checks:

- `resolved` → `selected_writer` present and matches a `procedure_name` in `candidate_writers`.
- `resolved` → selected writer candidate has `dependencies` populated (from Step 2 refs).
- `ambiguous_multi_writer` → at least two candidates, no `selected_writer`.

Set `validation.passed = false` and populate `validation.issues[]` on failure.

Write the final JSON (schema_version, run_id, results[], summary) to the output file path.

### Step 5 — Persist Resolved Statements to Catalog

After writing scoping output, persist resolved statements for each `resolved` item to `catalog/procedures/<selected_writer>.json`.

For each resolved item:

1. If `discover show` returned `classification: deterministic` — all statements already have `action: migrate|skip`. Write them with `source: "ast"`.

2. If `discover show` returned `classification: claude_assisted` — the LLM analysis in Step 2 resolved all `claude` actions to `migrate` or `skip`. Write them with `source: "llm"`.

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../lib" discover write-statements \
  --ddl-path <ddl_path> --name <selected_writer> --statements '<json>'
```

No `claude` actions are persisted — all must be resolved before writing.
