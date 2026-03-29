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

- **Input schema:** `../shared/shared/schemas/scope_input.json`
- **Output schema:** `../shared/shared/schemas/candidate_writers.json`

After reading the input, read `<ddl_path>/manifest.json` for `technology` and `dialect`. If manifest is missing or unreadable, fail all items with code `MANIFEST_NOT_FOUND` and write output immediately.

---

## Pipeline

### Step 1 — Discover Refs

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl_path> --name <item_id>
```

The output contains `writers` and `readers`. Each writer is a candidate.

If discover fails (exit code 1 or 2), record `error` with code `DISCOVER_EXECUTION_FAILED`.

### Step 2 — Enrich Each Candidate

For each candidate writer, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl_path> --name <writer>
```

From the output, extract `refs`, `classification`, and `raw_ddl`.

**Resolve to base tables:** If any ref is a view, function, or procedure — run `discover show` on it and follow the chain until you reach base tables. Assemble the fully resolved `dependencies: { tables, views, functions }`.

**Enrichment path based on `classification`:**

**`deterministic`**: `refs` and `statements` are complete. Use directly.

**`claude_assisted`**: Read `raw_ddl` from the show output. Analyse the proc body to determine `reads_from`, `writes_to`, and classify each statement as `migrate` or `skip`. For any EXEC calls to other procs, run `discover show` on the called proc and follow the chain recursively. Populate `llm_analysis` on the candidate with your findings. Add `LLM_ANALYSIS_REQUIRED` warning.

The `llm_analysis` field preserves your analysis so downstream agents (profiler, decomposer) don't re-read the same proc body.

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
