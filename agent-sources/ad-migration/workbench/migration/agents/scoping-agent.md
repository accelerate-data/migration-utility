---
name: scoping-agent
description: Identifies writer procedures from catalog data or static DDL files and produces a CandidateWriters JSON output. Use when scoping a migration item.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Scoping Agent

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable.

Use `uv run discover` directly for all analysis — do not invoke the discover skill.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../shared/shared/schemas/scope_input.json`
- **Output schema:** `../shared/shared/schemas/candidate_writers.json`

Key output fields to populate correctly:

- `result.analysis` / `candidate_writer.analysis` — `"deterministic"` unless LLM reasoning via `discover show` was needed; then `"claude_assisted"`.
- `candidate_writer.call_path` — always required; `["schema.proc"]` for direct candidates.
- `candidate_writer.confidence` — `1.0` for catalog-sourced writers; scored `[0.0, 1.0]` for AST-derived candidates.

After reading the input, read `<ddl_path>/manifest.json` for `technology` and `dialect`. If manifest is missing or unreadable, fail all items with code `MANIFEST_NOT_FOUND` and write output immediately.

---

## Pipeline

### Step 1 — Discover Refs

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl_path> --name <item_id> --depth <search_depth>
```

Check the `source` field in the output:

**`source: "catalog"`** — writers are procs/views with `is_updated=true`. Binary facts, no confidence scoring. Set `analysis: "deterministic"`, `confidence: 1.0` on each candidate.

**`source: "ast"`** — writers carry `write_type`, `write_operations`, `call_path`, `confidence`. Map deterministic writers with `analysis: "deterministic"`. `llm_required[]` procs need Step 2.

If discover fails, record `error` with code `DISCOVER_EXECUTION_FAILED`.

### Step 2 — Analyse LLM-Required Procs (AST only)

Skip on catalog path.

For each proc in `llm_required[]`, run `discover show` and read `raw_ddl` and `statements`. Determine whether it writes to the target table. If it does, produce a candidate with `analysis: "claude_assisted"`. Set `result.analysis: "claude_assisted"` for the item.

### Step 3 — Apply Resolution Rules

**Catalog path:**

| Condition | Status |
|---|---|
| One writer | `resolved` — set `selected_writer` |
| Two or more writers | `ambiguous_multi_writer` |
| No writers | `no_writer_found` |
| Discover failed | `error` |

**AST fallback path:**

| Condition | Status |
|---|---|
| One writer with confidence >= 0.70 | `resolved` — set `selected_writer` |
| Two or more writers with confidence >= 0.70 | `ambiguous_multi_writer` |
| Writers exist but all confidence < 0.70 | `partial` |
| No writers | `no_writer_found` |
| Discover failed | `error` |

**LLM-required warnings (catalog path):** For each candidate, read `<ddl_path>/catalog/procedures/<writer>.json`. If `needs_llm: true`, add to `warnings[]`:

```json
{ "code": "LLM_ANALYSIS_REQUIRED", "message": "Proc contains dynamic SQL or complex control flow — catalog references may be incomplete.", "severity": "warning" }
```

### Step 4 — Enrich Selected Writer with `dependencies`

For `resolved` items only:

**Catalog path:** Read `<ddl_path>/catalog/procedures/<writer>.json` and assemble `dependencies: { tables, views, functions }` from the `references` section.

**AST path:** Run `discover show` on the selected writer and extract `dependencies` from the JSON output.

### Step 5 — Validate and Write Output

Non-obvious cross-field checks:

- `resolved` → `selected_writer` present and matches a `procedure_name` in `candidate_writers`.
- `resolved` → selected writer candidate has `dependencies` populated.
- `ambiguous_multi_writer` → at least two candidates, no `selected_writer`.

Set `validation.passed = false` and populate `validation.issues[]` on failure.

Write the final JSON (schema_version, run_id, results[], summary) to the output file path.
