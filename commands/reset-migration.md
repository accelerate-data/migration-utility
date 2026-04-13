---
name: reset-migration
description: >
  Reset one or more tables from a chosen pre-model migration stage onward by
  clearing persisted catalog state and test-spec artifacts, then re-exposing
  the same stage gates on the next run.
user-invocable: true
argument-hint: "<scope|profile|generate-tests|refactor> <schema.table> [schema.table ...]"
---

# Reset Migration

Reset one or more selected tables from a chosen pre-model stage onward. This command is a thin wrapper over the deterministic `migrate-util reset-migration` CLI.

## Guards

- Require at least one target table.
- Supported stages are only: `scope`, `profile`, `generate-tests`, `refactor`.
- Do not run if any selected table already has model generation complete. Surface the CLI block result and stop without partial mutation.
- Treat repeated reset of already-cleared state as a valid no-op.

## Step 1 — Parse arguments

Interpret `$ARGUMENTS` as:

```text
<stage> <schema.table> [schema.table ...]
```

If the stage is missing or unsupported, explain the accepted syntax and stop.

## Step 2 — Preflight summary

Show the user exactly what will be reset before asking for confirmation:

- stage requested
- resolved target tables
- what downstream state will be cleared
- that generated dbt model artifacts are **not** removed
- that model-complete tables are rejected

Use this reset boundary:

- `scope` → clears table `scoping`, `profile`, `test_gen`, deletes `test-specs/<fqn>.json` if present, clears selected writer `refactor`
- `profile` → clears table `profile`, `test_gen`, deletes `test-specs/<fqn>.json` if present, clears selected writer `refactor`
- `generate-tests` → clears table `test_gen`, deletes `test-specs/<fqn>.json` if present, clears selected writer `refactor`
- `refactor` → clears selected writer `refactor` only

## Step 3 — Confirmation

Ask:

```text
Proceed with reset-migration <stage> for these tables? (y/n)
```

If the user declines, stop without changes.

## Step 4 — Run the CLI

Execute:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util reset-migration <stage> <table1> <table2> ...
```

Always pass the tables exactly as resolved in Step 2.

## Step 5 — Report

Present a concise summary:

```text
reset-migration complete

  reset:
    silver.DimCustomer

  no-op:
    silver.DimProduct

  blocked:
    silver.FactSales — model generation already complete
```

Also mention:

- any deleted `test-specs/<fqn>.json` files
- that the next run will require the reset stage again
- if the CLI returned blocked or not-found targets, do not claim success for those items
