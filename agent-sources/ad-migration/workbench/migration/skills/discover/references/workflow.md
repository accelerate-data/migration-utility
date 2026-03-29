# discover Workflow

Three subcommands: `list`, `show`, `refs`. The user may invoke any subcommand directly via `$ARGUMENTS` or navigate through them interactively. Use the `ddl-path` from `$ARGUMENTS` for all commands — never hardcode a path.

Requires catalog files from `setup-ddl`. If the catalog is missing, `discover` will error and tell the user to run `setup-ddl` first.

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

The output contains `type`, `raw_ddl`, `columns`, `params`, `refs`, `statements`, `needs_llm`, `classification`, and `parse_error`. Present differently based on the object type:

### Tables

Show the column list (from catalog):

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

- `needs_llm: false` → **Deterministic** — refs are complete (from catalog + enrichment), statements available
- `needs_llm: true` → **Claude-assisted** — refs may be partial, statements are null. Read `raw_ddl` directly.

**Deterministic example:**

```text
Classification: Deterministic

Call Graph

  silver.usp_load_DimCustomer  (direct writer)
    ├── reads: bronze.Customer
    ├── reads: bronze.Person
    └── writes: silver.DimCustomer  (UPDATE + INSERT)

Logic Summary
  1. TRUNCATE TABLE silver.DimCustomer
  2. INSERT INTO silver.DimCustomer from JOIN of bronze.Customer and bronze.Person
  3. Computes DateFirstPurchase via OUTER APPLY on bronze.SalesOrderHeader
```

**Claude-assisted example (dynamic SQL):**

```text
Classification: Claude-assisted (needs_llm)
Statements: not available — read raw_ddl below

  raw_ddl:
    CREATE PROCEDURE silver.usp_load_FactSales
    AS BEGIN
      DECLARE @sql NVARCHAR(MAX) = ...
      EXEC(@sql)
    END
```

For Claude-assisted procs, read the `raw_ddl` to complete the analysis. Identify writes, reads, and calls that the catalog could not resolve (dynamic SQL, complex control flow).

Use the same call graph format for both paths.

## refs

Find what references an object (from catalog data):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover refs \
  --ddl-path <ddl-path> --name <fqn>
```

The output splits callers into `readers` (is_selected only) and `writers` (is_updated). Data comes from `catalog/tables/<table>.json` → `referenced_by`. No AST parsing, no BFS, no confidence scoring — writers are facts from `sys.dm_sql_referenced_entities`.

Present grouped:

```text
silver.FactSales references (from catalog):

  Writers (1):
    - dbo.usp_load_FactSales  (is_updated)

  Readers (2):
    - dbo.usp_read_fact_sales  (is_selected)
    - dbo.vw_sales_summary  (is_selected)
```

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. This is an inherent limitation of `sys.dm_sql_referenced_entities` — it resolves references at definition time, not runtime. These procs require LLM analysis via `discover show`.
