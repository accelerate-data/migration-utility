# Stage 2 -- Profiling

The `/profile` command produces migration profiles for each table by answering six profiling questions. It launches one sub-agent per table in parallel, each running the `/profiling-table` skill.

## Prerequisites

- `manifest.json` must exist (if missing, all items fail with `MANIFEST_NOT_FOUND`)
- `catalog/tables/<item_id>.json` must exist for each table (if missing, item skipped with `CATALOG_FILE_MISSING`)
- `scoping.selected_writer` must be set in the catalog file (if missing, item skipped with `SCOPING_NOT_COMPLETED`)

## Invocation

Pass one or more fully-qualified table names:

```text
/profile silver.DimCustomer silver.DimProduct silver.FactInternetSales
```

## The Six Profiling Questions

Each sub-agent answers six questions by combining catalog signals (facts from `/setup-ddl`) with LLM inference from the writer procedure's code.

### Q1 -- Classification

Determines the table's role and materialization strategy. The profiler examines write patterns in the stored procedure and column shapes to classify the table.

| Classification | Typical proc pattern | dbt materialization |
|---|---|---|
| `dim_non_scd` | `TRUNCATE` + `INSERT` with descriptive columns | `table` |
| `dim_scd1` | `MERGE` with simple `WHEN MATCHED THEN UPDATE` | `table` or `incremental` |
| `dim_scd2` | `MERGE` that expires rows + inserts history | `incremental` (snapshot) |
| `dim_junk` | Cross-join `INSERT` of flag combinations | `table` |
| `fact_transaction` | Pure `INSERT`, no `UPDATE` or `DELETE` | `incremental` |
| `fact_periodic_snapshot` | `TRUNCATE` + `INSERT` with measure + FK columns | `table` |
| `fact_accumulating_snapshot` | `INSERT` then `UPDATE` targeting milestone dates | `incremental` |
| `fact_aggregate` | `INSERT ... SELECT ... GROUP BY` | `table` |

### Q2 -- Primary Key

Checks catalog `primary_keys` first (this is a fact, not a candidate). If no declared PK exists, falls back to MERGE ON clauses or UPDATE/DELETE WHERE patterns in the procedure body.

### Q3 -- Foreign Keys

Checks catalog `foreign_keys` first. For undeclared FKs, analyzes JOIN patterns in reader and writer procedures. Classifies each FK relationship:

- `standard` -- single fact column joins one dimension key
- `role_playing` -- two or more fact columns join the same dimension
- `degenerate` -- column acts as a business key with no dimension join target

### Q4 -- Natural Key vs Surrogate Key

Checks catalog `auto_increment_columns` first. If no identity column, looks for `NEWID()`, `NEWSEQUENTIALID()`, column name suffixes (`_sk` = surrogate, `_code` = natural), and MERGE ON vs INSERT PK patterns.

### Q5 -- Watermark

Identifies the column used for incremental loading. Looks for WHERE clauses in the proc body (`WHERE load_date > @last_run`), datetime column name patterns (`modified_at`, `updated_at`, `load_date`, `etl_date`), and CDC/change tracking metadata.

Watermark type maps to dbt strategy:

| Watermark type | dbt strategy |
|---|---|
| `datetime` / `datetime2` column | `microbatch` (preferred) or `merge` |
| Integer identity column | `merge` or `delete+insert` |
| Append-only, no updates | `append` |
| Date-partitioned table | `insert_overwrite` |

### Q6 -- PII Actions

Checks catalog `sensitivity_classifications` first. For remaining columns, uses name-pattern matching (`email`, `ssn`, `dob`, `phone`, `address`, etc.) and recommends actions: `mask`, `drop`, `tokenize`, or `keep`.

## Source Attribution

Every profiling answer carries a `source` field:

| Source | Meaning |
|---|---|
| `catalog` | Fact from `/setup-ddl` catalog data, not inferred |
| `llm` | Inferred by LLM from proc body, column patterns, or reference tables |
| `catalog+llm` | Catalog provided the base fact, LLM added classification |

## Output

Profile data is written to the `profile` section of each table's catalog file. The item result tracks status:

| Status | Meaning |
|---|---|
| `ok` | All required questions answered (classification, primary_key, watermark) |
| `partial` | One or more required questions could not be answered |
| `error` | Runtime failure prevented profiling |

## Batch Summary

After all sub-agents complete, the command presents:

```text
profile complete -- 3 tables processed

  ok silver.DimCustomer    ok
  ~  silver.DimProduct     partial (PARTIAL_PROFILE)
  !! silver.DimDate        error (SCOPING_NOT_COMPLETED)

  ok: 1 | partial: 1 | error: 1
```

The command then asks whether to commit and open a PR. If an existing open PR is found on the branch, it is updated instead of creating a new one.

## Error Codes

| Code | When |
|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` missing -- all items fail |
| `CATALOG_FILE_MISSING` | Catalog file not found -- item skipped |
| `SCOPING_NOT_COMPLETED` | No `selected_writer` in catalog -- item skipped |
| `PROFILING_FAILED` | `/profiling-table` skill pipeline failed -- item skipped |
| `PARTIAL_PROFILE` | LLM could not answer a required question -- item proceeds as partial |

## Next Step

Proceed to [[Stage 3 Test Generation]] to generate test scenarios and capture ground truth.
