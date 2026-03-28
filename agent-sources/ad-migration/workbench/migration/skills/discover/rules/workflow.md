# discover Workflow

Step sequence for the `discover` skill: list → show → refs.

## Step 1 — list

Run `discover list` to enumerate objects in the DDL artifact directory:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path ./artifacts/ddl --type <type>
```

Valid `--type` values: `tables`, `procedures`, `views`, `functions`.

Present the results as a numbered list and prompt the user to choose an object:

```text
Found 5 tables:
  1. dbo.DimCustomer
  2. dbo.DimProduct
  ...
Which table would you like to explore?
```

Wait for the user's selection before proceeding to Step 2.

## Step 2 — show

Run `discover show` to inspect the selected object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path ./artifacts/ddl --name <fqn>
```

Display the object's DDL and column list to the user.

If the output contains a `parse_error` field, surface a warning before continuing:

```text
Warning: <fqn> could not be fully parsed.
  Reason: <parse_error value>

Raw DDL is available for manual inspection. Proceeding.
```

Do not abort the workflow on parse errors — continue to the next step.

## Step 3 — refs

Run `discover refs` when the user asks what references the selected object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path ./artifacts/ddl --name <fqn>
```

Group entries in `referenced_by` by caller type before displaying:

- **Procedures** — DDL begins with `CREATE PROCEDURE` or `CREATE PROC`
- **Views** — DDL begins with `CREATE VIEW`

Note that `refs` reports a reference relationship only. Use the `scope` skill to determine which callers actually perform write operations.
