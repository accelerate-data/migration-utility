---
name: scope
description: >
  This skill should be used when the user asks to "find what writes to [table]", "which procedures populate [table]", "scope out writers for [table]", "identify the writer for [table]", or needs to determine which stored procedure is responsible for loading a specific target table.
argument-hint: "[ddl-path] [table-name]"
---

# Scope

Instructions for using `scope` to identify procedures that write to a target table.

## Arguments

Parse `$ARGUMENTS`:

- `ddl-path` (required): path to the directory containing `.sql` files
- `table-name` (required): fully-qualified target table (e.g. `dbo.FactSales`)

If either is missing from `$ARGUMENTS`, ask the user before proceeding. Do not assume `./artifacts/ddl` or any other default — the user chooses where their DDL lives.

## Invoking scope

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" scope \
  --ddl-path ./artifacts/ddl \
  --table dbo.FactSales \
  --dialect tsql \
  --depth 3
```

Flag defaults:

| Flag | Default | Description |
|---|---|---|
| `--ddl-path` | _(required)_ | Path to the DDL artifact directory |
| `--table` | _(required)_ | Fully-qualified target table name |
| `--dialect` | `tsql` | SQL dialect of the source procedures |
| `--depth` | `3` | Maximum call-graph depth to traverse |

Output shape:

```json
{
  "table": "dbo.FactSales",
  "writers": [
    {
      "procedure_name": "dbo.usp_LoadFactSales",
      "write_type": "direct",
      "write_operations": ["INSERT", "MERGE"],
      "call_path": ["dbo.usp_LoadFactSales"],
      "confidence": 0.90,
      "status": "confirmed"
    },
    {
      "procedure_name": "dbo.usp_StageLoad",
      "write_type": "indirect",
      "write_operations": ["INSERT"],
      "call_path": ["dbo.usp_LoadFactSales"],
      "confidence": 0.65,
      "status": "suspected"
    }
  ],
  "errors": [
    {
      "procedure": "dbo.usp_CrossDbSync",
      "code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE",
      "message": "Procedure references a table in a different database."
    }
  ]
}
```

## Confidence thresholds

Each writer entry has a `confidence` score in [0.0, 1.0] and a derived `status`:

| Status | Condition | Action |
|---|---|---|
| `confirmed` | confidence ≥ 0.70 | Proceed — treat this procedure as a definite writer |
| `suspected` | confidence < 0.70 | Do not proceed automatically — escalate |

Scoring signals (computed deterministically, not by LLM):

| Signal | Effect |
|---|---|
| Direct write evidence (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`) | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop shorter than the deepest path in the candidate set) | +0.02 |
| Multiple independent paths all show write evidence | +0.05 |
| Dynamic SQL present alongside static write evidence (`EXEC(@sql)`, `sp_executesql`) | −0.20 |
| Only dynamic SQL evidence — no static write statement found | cap at 0.45 |

## Escalation rule

When a writer entry has `"status": "suspected"`, do not include it in the migration plan automatically. Instead:

1. Read the procedure body using the `show` subcommand of `discover`:

   ```bash
   uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show --ddl-path ./artifacts/ddl --name dbo.usp_StageLoad
   ```

   Alternatively, locate and read the raw DDL file directly.

2. Manually inspect the procedure body to determine whether it actually writes to the target table.

3. Based on inspection, confirm or reject the procedure:
   - **Confirmed:** include it in the migration plan and proceed.
   - **Rejected:** exclude it and note the reason for the user.

4. Only proceed to migration steps after a decision is made for every `suspected` entry.

Present the escalation to the user before inspecting, so they are aware a manual check is required:

```text
dbo.usp_StageLoad has confidence 0.65 (suspected).
Inspecting procedure body to verify whether it writes to dbo.FactSales...
```

## Cross-DB error entries

When `errors[]` contains an entry with `"code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE"`:

1. Surface the affected procedure name to the user.
2. Mark it as out-of-scope for this migration.
3. Do not include it in the migration plan.
4. Note that cross-database writes may require a separate data pipeline migration outside this workflow.

Example display:

```text
Out-of-scope procedure detected:
  dbo.usp_CrossDbSync — references a table in a different database.
  This procedure is excluded from the migration plan.
  Cross-database writes may need a separate data pipeline migration.
```
