# Glossary

Domain terms used across the migration utility wiki.

## Project structure

- **manifest** -- `manifest.json` in the project root. Contains runtime role configuration, extraction metadata, and sandbox configuration. Created and updated by the setup commands.

- **catalog** -- the `catalog/` directory tree containing per-object JSON files organized by type (`tables/`, `procedures/`, `views/`, `functions/`). This is the shared state across all pipeline stages. Every stage reads from and writes to catalog files.

- **item_id** -- the canonical identifier for one table migration: `<schema>.<table>` (e.g. `silver.DimCustomer`). One table equals one migration item equals one dbt model. Used as the filename stem in catalog files, test specs, and run logs.

- **run log** -- the `.migration-runs/` directory containing ephemeral per-command execution metadata (timing, cost, per-item status). Each file includes a Unix epoch suffix so runs accumulate without overwriting. `.gitignore`d and never committed. Consumed at commit/PR time for rich messages.

- **worktree** -- a git worktree created by batch commands for parallel execution. Lives at `../worktrees/<branchName>` relative to the repo root. Allows the FDE to run multiple batch commands simultaneously without conflicts.

## Scoping

- **selected_writer** -- the stored procedure identified during scoping as the primary writer for a table. Determined by analyzing `refs` (which procedures INSERT, UPDATE, MERGE, or DELETE into the table) and resolving candidates. Written to the table catalog's `scoping` section.

- **routing flags** -- `mode` and `routing_reasons` fields on procedure catalog entries. Determine how the procedure is processed during analysis. `discover show` exposes these as the `needs_llm` boolean and `routing_reasons` array.

- **needs_llm** -- boolean field on `discover show` output indicating whether the procedure requires LLM reasoning. `false` means the AST engine (sqlglot) fully parsed all statements. `true` means the procedure contains dynamic SQL, unparseable constructs, or parse failures that require the LLM to read `raw_ddl` and classify statements.

## Profiling

- **classification** -- the table type determined during profiling. Values: `dim_non_scd`, `dim_scd1`, `dim_scd2`, `dim_junk`, `fact_transaction`, `fact_periodic_snapshot`, `fact_accumulating_snapshot`, `fact_aggregate`. Drives materialization strategy and model structure.

- **materialization** -- the dbt materialization strategy mapped from the table classification. For example, `dim_scd2` maps to `snapshot`, `fact_transaction` maps to `incremental`, `dim_non_scd` maps to `table`.

- **signal queries** -- 12 catalog enrichment queries run during DDL extraction (`/setup-ddl`). These queries discover primary keys, foreign keys, identity columns, CDC tracking, change tracking, sensitivity labels, DMF references, and other structural metadata. The results are stored in catalog files and consumed by profiling and downstream stages.

## Test generation

- **test spec** -- `test-specs/<item_id>.json`, the output of the test generation stage. Contains the branch manifest, unit test definitions, fixture data, and ground truth results. Consumed by the model generator to render `unit_tests:` in the dbt schema YAML.

- **branch manifest** -- the list of conditional branches enumerated from a stored procedure's body. Each branch represents a distinct execution path (e.g. IF/ELSE conditions, CASE expressions). Used by the test generator to ensure test coverage spans all paths.

- **ground truth** -- actual output rows produced by executing a stored procedure in the sandbox with test fixtures. These rows are the expected results that the generated dbt model must reproduce. Captured during test generation and stored in the test spec.

- **sandbox** -- a throwaway database (`__test_<random_hex>`) created by `/setup-sandbox` for executing stored procedures during test generation. Cloned from the source SQL Server's schema and procedures. Torn down via `/teardown-sandbox` after test generation is complete.

## Migration

- **CTE pattern** -- the dbt model structure used for generated models: staging CTE (selecting from source) followed by transformation CTEs (one per logical step) followed by a final SELECT. This pattern keeps models readable and testable.

## Diagnostics

- **diagnostics** -- a standardized error/warning format used across the pipeline. Each entry contains `code` (stable machine-readable identifier), `message` (human-readable description), `item_id` (the affected table), `severity` (`error` or `warning`), and an optional `details` object. Stored in catalog files as `diagnostics_entry` arrays.

- **error codes** -- stable identifiers for guard failures and pipeline errors. Examples: `MANIFEST_NOT_FOUND`, `SCOPING_NOT_COMPLETED`, `PROFILE_NOT_COMPLETED`. See [[Troubleshooting and Error Codes]] for the full index.

## Related pages

- [[Status Dashboard]] -- how guard checks use these concepts
- [[Browsing the Catalog]] -- exploring catalog contents interactively
- [[Troubleshooting and Error Codes]] -- full error code reference
