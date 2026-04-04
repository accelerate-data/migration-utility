# Quickstart

Happy-path walkthrough migrating two tables (`silver.DimCustomer` and `silver.FactInternetSales`) from SQL Server stored procedures to dbt models. Each step runs one command with minimal explanation. See the linked stage pages for full details.

## Prerequisites

- All tools installed and verified (see [[Installation and Prerequisites]])
- A migration project repo initialized with git
- SQL Server accessible with MSSQL environment variables set
- `toolbox` binary on PATH

## Step 1 -- Scaffold the project

```text
/init-ad-migration
```

Checks prerequisites, installs missing dependencies, and scaffolds project files (`CLAUDE.md`, `README.md`, `repo-map.json`, `.gitignore`, `.envrc`, `.githooks/`). Safe to re-run.

See [[Stage 1 Project Init]] for details.

## Step 2 -- Extract DDL and catalog

```text
/setup-ddl
```

Connects to your SQL Server, walks you through database and schema selection, extracts DDL for all objects, and builds catalog files with 12 signal queries. Produces `manifest.json`, `ddl/*.sql`, and `catalog/**/*.json`.

See [[Stage 2 DDL Extraction]] for details.

## Step 3 -- Scaffold dbt project

```text
/init-dbt
```

Reads the manifest and catalog, asks you to pick a target platform (Fabric Lakehouse, Spark, Snowflake, or DuckDB), and scaffolds a dbt project with `sources.yml` generated from your catalog tables.

See [[Stage 3 dbt Scaffolding]] for details.

## Step 4 -- Scope tables

```text
/scope silver.DimCustomer silver.FactInternetSales
```

Discovers which stored procedures write to each table. Launches one sub-agent per table in parallel, analyzes candidate writers, and writes the `selected_writer` to each table's catalog file. Opens a PR with the results.

See [[Stage 4 Scoping]] for details.

## Step 5 -- Profile tables

```text
/profile silver.DimCustomer silver.FactInternetSales
```

Classifies each table (dimension vs. fact, SCD type), identifies primary keys, foreign keys, natural keys, watermark columns, and PII. Writes profile answers to catalog files and opens a PR.

See [[Stage 5 Profiling]] for details.

## Step 6 -- Create sandbox

```text
/setup-sandbox
```

Creates a throwaway database (`__test_<run_id>`) by cloning schema and procedures from your source SQL Server. The sandbox is used by the test generator to execute procs and capture ground truth output.

See [[Stage 6 Test Generation]] for details.

## Step 7 -- Generate tests

```text
/generate-tests silver.DimCustomer silver.FactInternetSales
```

Enumerates branches in each stored procedure, synthesizes fixture data, executes the proc in the sandbox, and captures ground truth output. Includes a review loop for coverage quality. Opens a PR with the approved test specs.

See [[Stage 6 Test Generation]] for details.

## Step 8 -- Generate dbt models

```text
/generate-model silver.DimCustomer silver.FactInternetSales
```

Generates dbt models from the stored procedures using the profile and test spec. Runs `dbt test` with up to 3 self-corrections, then a code review loop with up to 2 iterations. Opens a PR with the generated models.

See [[Stage 7 Model Generation]] for details.

## Step 9 -- Tear down sandbox

```text
/teardown-sandbox
```

Drops the throwaway sandbox database. This is a destructive operation and requires confirmation.

See [[Stage 6 Test Generation]] for details.

## What's Next

- Review and merge the PRs opened by each batch command
- Run `/cleanup-worktrees` to remove stale worktrees after PRs are merged
- Repeat steps 4-8 for additional tables in your migration scope
