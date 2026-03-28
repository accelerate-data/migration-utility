# scope Workflow

Step sequence for the `scope` skill.

## Step 1 — Invoke

Run `scope` against the target table:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" scope \
  --ddl-path ./artifacts/ddl \
  --table <fqn> \
  --dialect tsql \
  --depth 3
```

`--table` is required. `--dialect` defaults to `tsql`. `--depth` defaults to 3.

## Step 2 — Evaluate confidence

For each entry in `writers[]`, check the `status` field:

| Status | Condition | Action |
| --- | --- | --- |
| `confirmed` | confidence ≥ 0.70 | Include in migration plan and proceed |
| `suspected` | confidence < 0.70 | Do not proceed automatically — escalate |

## Step 3 — Escalate suspected entries

For each `suspected` writer, notify the user before inspecting:

```text
<fqn> has confidence <score> (suspected).
Inspecting procedure body to verify whether it writes to <table>...
```

Then use the `discover` skill's `show` subcommand to read the procedure body:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path ./artifacts/ddl --name <procedure-fqn>
```

Inspect the body manually. Based on inspection:

- **Confirmed:** include in migration plan and proceed.
- **Rejected:** exclude and note the reason for the user.

Do not proceed to migration steps until every `suspected` entry has a decision.

## Step 4 — Handle cross-DB errors

When `errors[]` contains an entry with
`"code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE"`:

1. Surface the affected procedure name to the user.
2. Mark it as out-of-scope for this migration.
3. Exclude it from the migration plan.
4. Note that cross-database writes may require a separate data pipeline
   migration outside this workflow.

## Step 5 — Report

Present the final writer list with procedure name, status, confidence score,
and write operations. Distinguish `confirmed` from any manually-confirmed
`suspected` entries. Note any excluded out-of-scope procedures.
