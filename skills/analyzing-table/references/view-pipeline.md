# View Pipeline

Follow this reference after the parent skill has confirmed:

- `/scope` readiness passed
- the object is a view or materialized view
- shared write rules from the parent skill are in force

## Step 1 -- Show view from catalog

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover show \
  --name <view_fqn>
```

Read `catalog/views/<view_fqn>.json` to get `is_materialized_view` and `references.views.in_scope`.

Present the object type and, for materialized views, column count:

```text
silver.vw_CustomerSales (view)
```

If `errors` contains `DDL_PARSE_ERROR` and `sql_elements` is null, proceed using `raw_ddl` for later steps and preserve the canonical diagnostic in the persisted output.

## Step 2 -- Build call tree

Resolve sources from:

- `refs.reads_from` for source tables
- `references.views.in_scope` for source views

Example:

```text
Call tree for silver.vw_CustomerSales:

  Reads tables:  bronze.Customer, bronze.Person
  Reads views:   silver.vw_AddressBase
```

If the view depends on in-scope views, add a canonical warning entry to the scoping output.

## Step 3 -- Identify SQL elements

If `sql_elements` is populated, present it directly.

If `sql_elements` is null, read `raw_ddl` and identify the same feature types manually:

- joins
- group by
- aggregations
- window functions
- case expressions
- subqueries
- CTEs

Use [statement-classification.md](statement-classification.md) only if dialect-specific classification detail is needed during manual inspection.

## Step 4 -- Logic summary

Read `raw_ddl` and write a 2-4 sentence plain-language description of what the view computes.

## Step 5 -- Persist scoping

Create the temp file first, then persist it to the catalog:

```bash
mkdir -p .staging
cat > .staging/scoping.json <<'EOF'
<scoping JSON>
EOF
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" discover write-scoping \
  --name <view_fqn> --scoping-file .staging/scoping.json && rm -rf .staging
```

`discover write-scoping` reads `.staging/scoping.json` and persists that scoping payload into the catalog.

Required fields:

- `sql_elements`
- `call_tree`
- `logic_summary`
- `rationale`
- `warnings`
- `errors`

If there was a parse failure, keep the existing `DDL_PARSE_ERROR` entry in `errors`.

## Step 6 -- Present persisted result

Present:

- call tree
- SQL elements
- logic summary
- `VIEW_DEPENDS_ON_VIEWS` warning prominently, if present

## Common mistakes

- Do not drop parse diagnostics just because manual analysis succeeded.
- Do not leave in-scope view dependencies only in prose. Persist them as warnings.
- Do not switch to dialect references unless `sql_elements` is unavailable or insufficient.
