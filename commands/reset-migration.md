---
name: reset-migration
description: >
  Reset one or more tables from a chosen pre-model migration stage, or reset
  the entire migration workspace with an inventory-first destructive flow.
user-invocable: true
argument-hint: "<scope|profile|generate-tests|refactor> <schema.table> [schema.table ...] | all"
---

# Reset Migration

Reset one or more selected tables from a chosen pre-model stage onward, or reset the entire workspace with `all`. This command is a thin wrapper over the deterministic `migrate-util reset-migration` CLI.

## Guards

- Supported stages are only: `scope`, `profile`, `generate-tests`, `refactor`, and `all`.
- Stage mode requires at least one target table.
- Global mode requires a destructive confirmation after the inventory summary.
- Do not run if any selected table already has model generation complete. Surface the CLI block result and stop without partial mutation.
- Treat repeated reset of already-cleared state as a valid no-op.
- If `all` is requested and `runtime.sandbox` is configured, sandbox teardown runs first as a command-layer precondition.
- The core CLI owns mode-specific validation; this command should pass the resolved arguments through and let the CLI decide on `all` vs staged behavior.

## Step 1 — Parse arguments

Interpret `$ARGUMENTS` as either:

```text
<stage> <schema.table> [schema.table ...]
```

or:

```text
all
```

If the mode is missing or unsupported, explain the accepted syntax and stop.

## Step 2 — Preflight summary

Show the user exactly what will be reset before asking for confirmation:

- For stage mode:
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

- For global mode:
  - print `runtime.sandbox: configured` or `runtime.sandbox: not configured` exactly
  - whether `catalog/`, `ddl/`, `.staging/`, `test-specs/`, and `dbt/` are `present` or `absent`
  - which manifest sections are present and will be cleared: `runtime.source`, `runtime.target`, `runtime.sandbox`, `extraction`, `init_handoff`
  - that sandbox teardown runs first when configured
  - that this reset is destructive and requires explicit confirmation
  - that the next required step after success is `/setup-ddl`

## Step 3 — Confirmation

Ask for stage mode:

```text
Proceed with reset-migration <stage> for these tables? (y/n)
```

Ask for global mode:

```text
Proceed with destructive reset-migration all for this workspace? Type DELETE ALL to continue.
```

If the user declines, stop without changes.

## Step 4 — Run the CLI

Execute:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util reset-migration <stage> --fqn <table1> --fqn <table2> ...
```

For global mode, run sandbox teardown first when `runtime.sandbox` is configured:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-down
```

If sandbox teardown fails, stop and do not run the reset CLI after it.

For global mode, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util reset-migration all
```

Always pass the tables exactly as resolved in Step 2 for stage mode.

## Step 5 — Report

Present a concise summary:

```text
reset-migration complete

  reset:
    silver.DimCustomer

  noop:
    silver.DimProduct

  blocked:
    silver.FactSales — model generation already complete
```

Start the final report with the literal command name `reset-migration` and the requested stage or `all`.
Preserve the resolved table names exactly as provided; do not normalize their case in the user-facing summary.
Use the exact status labels `reset`, `noop`, and `blocked` in the final summary.

Also mention:

- any deleted `test-specs/<fqn>.json` files
- that the next run will require the reset stage again
- if the CLI returned blocked or not-found targets, do not claim success for those items
- for global mode, that `/setup-ddl` is the next required step
- for global mode, restate the evidence-based inventory using the literal words `present` and `absent` for each tracked path
- for global mode, include explicit headings `deleted paths`, `missing paths`, and `cleared manifest sections` even when one of those lists is empty
