# discover Workflow

Three subcommands: `list`, `show`, `refs`. The user may invoke any subcommand directly via `$ARGUMENTS` or navigate through them interactively. Use the `ddl-path` from `$ARGUMENTS` for all commands — never hardcode a path.

## list

Enumerate objects in the DDL directory:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path <ddl-path> --type <type>
```

Valid `--type` values: `tables`, `procedures`, `views`, `functions`.

Present the results as a numbered list:

```text
Found 5 tables:
  1. dbo.DimCustomer
  2. dbo.DimProduct
  3. dbo.DimDate
  4. silver.FactSales
  5. silver.FactReturns

Which object would you like to inspect?
```

If the user selects an object, proceed to `show`. If they ask what references an object, proceed to `refs`.

## show

Inspect a single object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover show \
  --ddl-path <ddl-path> --name <fqn>
```

The output contains `type`, `raw_ddl`, `columns`, `params`, `refs`, and `parse_error`. Present differently based on the object type:

### Tables

Show the column list:

```text
silver.DimCustomer (table, 3 columns)

  CustomerKey   BIGINT       NOT NULL
  FirstName     NVARCHAR(50) NULL
  Region        NVARCHAR(50) NULL
```

### Views

Show the refs (what it reads from) and the view definition:

```text
silver.vw_CustomerSales (view)

  Reads from: silver.DimCustomer, silver.FactSales

  Definition:
    SELECT c.FirstName, SUM(f.Amount) AS TotalSales
    FROM silver.DimCustomer c
    JOIN silver.FactSales f ON c.CustomerKey = f.CustomerKey
    GROUP BY c.FirstName
```

### Procedures

Always include classification, call graph, and logic summary.

Check `needs_llm` and `classification` to determine the analysis tier (see parse classification table in SKILL.md):

- `needs_llm: false` → **Deterministic** — refs are complete, use directly
- `needs_llm: true` → **Claude-assisted** — refs may be partial (control flow or EXEC not captured by single-pass)

**Deterministic example:**

```text
Classification: Deterministic

Call Graph

  silver.usp_load_DimCustomer  (direct writer)
    ├── reads: bronze.Customer
    ├── reads: bronze.Person
    └── writes: silver.DimCustomer  (TRUNCATE + INSERT)

Logic Summary
  1. TRUNCATE TABLE silver.DimCustomer
  2. INSERT INTO silver.DimCustomer from JOIN of bronze.Customer and bronze.Person
  3. Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

**Claude-assisted example (EXEC orchestrator):**

```text
Classification: Claude-assisted (EXEC detected)
Parse error: Cannot extract refs: body contains unparsed statement(s)

Call Graph

  silver.usp_load_FactSales  (orchestrator, no direct DML)
    └── EXEC silver.usp_stage_FactSales  (leaf, truncate + INSERT)
          ├── reads: bronze.SalesOrderHeader
          ├── reads: bronze.SalesOrderDetail
          └── writes: silver.FactInternetSales

Logic Summary
  1. Calls usp_stage_FactSales via EXEC — orchestrator with no direct writes
```

For Claude-assisted procs, read the `raw_ddl` and `statements` to complete the analysis. Identify writes, reads, and calls that single-pass parsing missed (inside IF/ELSE, TRY/CATCH, or EXEC). If a called proc exists in the same DDL directory, run `show` on it to get its refs and build the call graph from that.

Use the same call graph format for both paths.

## refs

Find what references an object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl-path> --name <fqn>
```

The output splits callers into `readers` (FROM/JOIN), `writers` (INSERT/UPDATE/DELETE/MERGE/TRUNCATE/SELECT INTO), and optionally `llm_required` (procs with partial or missing refs due to control flow or EXEC).

Present grouped:

```text
silver.FactSales references:

  Writers (1):
    - silver.usp_load_FactSales  (deterministic)

  Readers (1):
    - silver.vw_SalesSummary  (deterministic)

  LLM required (1):
    - silver.usp_conditional_loader  (needs_llm — IF/ELSE control flow)
```

### Completing LLM-required refs

For each proc in `llm_required`, run `discover show` to get its `raw_ddl` and `statements`. Read the procedure body, identify whether it reads from or writes to the target, and report your finding alongside the deterministic results. This ensures the caller gets a complete picture without needing a separate scope pass.
