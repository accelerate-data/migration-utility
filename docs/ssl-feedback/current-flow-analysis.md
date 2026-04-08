# Current Migration Pipeline Analysis

## Overview

The migration utility is a Claude Code plugin that migrates SQL Server stored procedures to dbt models on Fabric Lakehouse. The current pipeline has 8 discrete steps, each invoked as a separate user command with its own approval gates.

This document captures the as-is pipeline so we can identify where automation should replace manual orchestration.

## Pipeline Steps

### 1. Init (`/init-ad-migration`)

Scaffolds the project structure: `CLAUDE.md`, `README`, `repo-map.json`, `manifest.json`. Prompts the user to select a source technology (SQL Server, Oracle, Fabric Warehouse). Installs dependencies. Commits to main.

### 2. Setup DDL (`/setup-ddl`)

Extracts DDL from the live database. User selects a database, then schemas. Writes `ddl/*.sql` flat files and `catalog/*.json` per-object files. Updates `manifest.json` with extraction metadata.

Each catalog JSON contains: columns, primary keys, foreign keys, auto-increment flags, and cross-references.

### 3. Scope (`/scope <table>`)

- **Tables**: Discovers writer procedures, handles multi-table writer disqualification, user selects the writer.
- **Views**: Analyzes SQL elements, builds the call tree, generates a logic summary.

The user specifies which tables to scope. Multi-table writers are hard-disqualified (a single procedure writing to multiple tables cannot be selected).

### 4. Profile (`/profile <table>`)

Answers 6 profiling questions per table:

| Question | Example Values |
|---|---|
| Classification | dim, fact, fact_periodic_snapshot, mart |
| Primary key type | surrogate, natural, composite |
| Natural key | specific column(s) |
| Watermark column | column used for incremental loads |
| Foreign keys | referenced tables |
| PII actions | mask, hash, drop, or none |

Each answer carries a `source` field (`catalog`, `llm`, or `catalog+llm`) indicating how it was derived. User approves the full profile before it persists.

### 5. Generate Tests (`/generate-tests <table>`)

Creates branch-covering test fixtures. Executes against a sandbox environment. Captures ground truth expected rows. Outputs `test-specs/*.json`.

### 6. Refactor (`/refactor <table>`)

Restructures the source procedure SQL into an import/logical/final CTE pattern suitable for dbt. Runs an equivalence audit against the original. Self-corrects up to 3 times on audit failure. Writes `refactored_sql` back to the catalog entry.

### 7. Init dbt (`/init-dbt`)

Scaffolds the dbt project: `dbt_project.yml`, `profiles.yml`, `packages.yml`, `models/`, `snapshots/`. Prompts the user to select a target platform (Fabric, Spark, Snowflake, SQL Server, DuckDB). Generates `sources.yml` from the catalog.

### 8. Generate Model (`/generate-model <table>`)

Generates `stg_*` ephemeral models plus mart models from the refactored SQL. Applies materialization rules based on the profile classification. Runs `dbt compile` and `dbt test`. Enters a review loop (max 2 iterations) if compilation or tests fail.

## The Problem: Wizard-Driven Interaction

Every step requires the user to:

1. Know which command to run next.
2. Specify which objects to operate on.
3. Approve intermediate results before proceeding.
4. Manually resolve prerequisite failures.

**Evidence from session 157796ea**: A user attempted to run `/refactoring-sql` for `gold.rpt_product_performance`. The entire session was spent fighting 4 prerequisite gates -- missing statements analysis, missing profile, missing sandbox, missing test spec -- instead of the agent resolving these dependencies itself.

The pipeline treats each step as a separate wizard with its own validation gates, rather than a continuous agentic process that resolves its own dependency graph.

## Batch Support (Partial)

Commands do support processing 2+ items with parallel sub-agents, but the user must still:

- Specify the batch (which tables).
- Choose the command to run.
- Approve before execution.
- Review after completion.

A batch planner exists that computes dependency-ordered execution batches, but it is only used within a single command invocation -- not across the full pipeline. There is no mechanism today that takes "migrate these 5 tables" and walks the entire graph from scoping through model generation without manual intervention at each boundary.

## Implications

The current design made sense during development when each step needed human validation. As the individual steps have stabilized, the manual orchestration layer is now the primary source of friction. The next iteration should let users express intent at the pipeline level ("migrate table X") and have the system resolve, execute, and checkpoint each phase autonomously -- surfacing approval gates only where the confidence threshold demands it.
