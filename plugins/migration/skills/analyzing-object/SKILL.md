---
name: analyzing-object
description: >
  Analyse a stored procedure for migration. Resolves call graphs, classifies statements, produces a logic summary and migration guidance, and persists results to catalog. Procedures only — use /listing-objects for views and functions.
user-invocable: true
argument-hint: "<schema.procedure>"
---

# Analyzing Object

Deep-dive analysis of a single stored procedure. Produces call graph, statement classification, logic summary, migration guidance, and persists resolved statements to catalog.

## Arguments

`$ARGUMENTS` is the fully-qualified procedure name (e.g. `dbo.usp_load_DimCustomer`). Use `AskUserQuestion` if missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the user to run `setup-ddl` first.
2. Confirm the object is a procedure by checking that `catalog/procedures/<name>.json` exists. If the object is a view, function, or table, tell the user to use `/listing-objects show <name>` instead and stop.

## Pipeline

### Step 1 — Fetch object data

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover show \
  --name <proc>
```

This returns `refs`, `statements`, `classification`, and `raw_ddl`.

### Steps 2–5 — Analyse and persist

Follow the canonical flow in [`references/procedure-analysis-flow.md`](references/procedure-analysis-flow.md):

1. Classify statements (deterministic vs claude-assisted)
2. Resolve call graph to base tables
3. Logic summary (plain-language)
4. Migration guidance (tag each statement `migrate` or `skip`)
5. Persist resolved statements to catalog

For classification guidance, see [`references/tsql-parse-classification.md`](references/tsql-parse-classification.md).

Persist command:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" discover write-statements \
  --name <procedure_name> --statements '<json>'
```

## Error handling

- `discover show` exits with code 2: catalog directory unreadable. Report and stop.
- Procedures with `parse_error`: still loaded — `raw_ddl` preserved. Report the parse error and proceed with `raw_ddl`-based analysis.
- Circular call graph: stop recursion and report the cycle.
- Unresolvable dynamic SQL (variable target table, external input): report as unresolvable.
