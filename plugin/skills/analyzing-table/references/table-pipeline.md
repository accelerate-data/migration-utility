# Table Pipeline

Follow this reference after the parent skill has confirmed:

- `/scope` readiness passed
- the object is a table, not a view
- shared write rules from the parent skill are in force

## Step 1 -- Show columns from catalog

Read `catalog/tables/<table>.json` and present the column list:

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   INTEGER      NOT NULL
  FirstName     VARCHAR(50)  NULL
  Region        VARCHAR(50)  NULL
```

## Step 2 -- Discover writer candidates

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover refs \
  --name <table>
```

Extract the `writers` array from the output.

If no writers are found, persist `no_writer_found`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping '{"selected_writer": null, "selected_writer_rationale": "No procedures found that write to this table."}'
```

Then ask the user:

> No writer found for `<table>`. Mark as a dbt source? (y/n)

If **y**, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-source \
  --name <table> --value
```

If **n**, stop. The table will remain pending source confirmation.

## Step 3 -- Analyze each writer candidate

For each writer candidate, follow [procedure-analysis.md](procedure-analysis.md).

Requirements:

- complete all 6 steps for every candidate
- persist statements for rejected candidates too
- analyze candidates sequentially

## Step 4 -- Present writer candidates

After all candidates are analyzed, present a summary:

```text
Writer candidates for silver.DimCustomer:

  1. dbo.usp_load_dimcustomer_full (direct writer)
     Reads: bronze.Customer, bronze.Person
     Writes: silver.DimCustomer
     Statements: 1 migrate, 1 skip

  2. dbo.usp_load_dimcustomer_delta (direct writer)
     Reads: bronze.Customer, silver.DimCustomer
     Writes: silver.DimCustomer
     Statements: 1 migrate (MERGE)
```

Include rationale, dependencies, and statement summary for each candidate.

## Step 5 -- Resolve final writer

Use the normal path directly:

- 1 clearly defensible writer -> select it
- 2+ writers -> compare candidates and resolve the best-supported writer
- 0 writers -> already handled in Step 2

Open [table-writer-resolution.md](table-writer-resolution.md) for:

- multi-table writers
- ambiguous multi-writer outcomes
- remote or linked-server `EXEC`
- opaque or unresolved write paths
- payload shape and no-selection outcomes

## Step 6 -- Persist scoping

Write the scoping JSON to a temp file:

```bash
mkdir -p .staging
# Write scoping JSON to .staging/scoping.json
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <table> --scoping-file .staging/scoping.json && rm -rf .staging
```

The payload must include `selected_writer_rationale`, even when no writer is selected.

If the write exits non-zero, report the error and stop for correction.

## Common mistakes

- Do not skip candidate statement persistence just because a candidate is later rejected.
- Do not decide multi-table or remote-delegate cases from intuition. Use [table-writer-resolution.md](table-writer-resolution.md).
- Do not use legacy candidate fields such as `procedure`, `write_type`, or `selected`.
