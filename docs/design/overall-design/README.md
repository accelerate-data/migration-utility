# Overall Design

Migration utility for stored-procedure-to-dbt conversion: a Claude Code plugin for interactive migration, a `migrate-util` CLI for batch execution, and GitHub Actions workflows for headless runs. Targets silver and gold transformations only (bronze is out of scope).

## Supported Sources

| Technology | `technology` value | Import format | Test generator access |
|---|---|---|---|
| SQL Server | `sql_server` | `.dacpac` | Docker + SQL Server container (GH Actions) |
| Fabric Warehouse | `fabric_warehouse` | `.zip` (DDL export) | T-SQL cloud endpoint |
| Fabric Lakehouse | `fabric_lakehouse` | `.zip` (DDL export) | Spark SQL |
| Snowflake | `snowflake` | `.zip` (DDL export) | SQL cloud connection |

---

## Prerequisites

1. **GitHub account** with `gh` CLI authenticated (`gh auth login`). The migration repo lives here; GHA workflows execute agent runs.
2. **Claude Code CLI** installed and authenticated.
3. **`ad-migration` plugin** installed in Claude Code (marketplace package containing bootstrap, migration, and ground-truth-harness plugins).

---

## High-Level Architecture

```text
Three execution paths, one pipeline:

┌─────────────────────────────────────────────────────────┐
│                   Migration Repository                  │
│  (1 project = 1 repo, all state in artifact JSONs)      │
└────────────────────────┬────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   Interactive      Local Batch       GHA Batch
   (FDE + Claude    (migrate-util     (migrate-util
    Code skills)     scope --table)    dispatch --all)
        │                │                │
        ▼                ▼                ▼
   Approval gates    Autonomous       Autonomous
   at every step     single-table     full-scope
```

| Path | Entry point | Approval gates | Runs where | Status |
|---|---|---|---|---|
| Interactive | FDE opens Claude Code, uses skills (`/discover-objects`, `/profile-table`, `/generate-model`) | Yes -- every step | Local terminal | Implemented |
| Local batch | `migrate-util scope --table X` | None -- agent runs autonomously | Local terminal | **Not yet implemented** |
| GHA batch | `migrate-util dispatch scope --all` | None -- commits input, triggers GHA workflow | GitHub Actions | **Not yet implemented** |

All three paths share the same deterministic Python skills and agent contracts. The difference is orchestration: interactive stops for FDE approval, batch continues autonomously.

---

## Migration Repository

One project per repo. No multi-project support, no slugs, no project IDs.

### Directory Layout

```text
manifest.json                  # dialect config (default: tsql)
catalog/
  tables/
    dbo.FactSales.json         # table catalog (keys, FKs, PII, profile answers)
    dbo.DimCustomer.json
  procedures/
    dbo.usp_Load_FactSales.json  # procedure catalog (statements, writer refs)
ddl/
  tables.sql                   # CREATE TABLE statements (filenames are conventional — loader detects object types from CREATE statements)
  procedures.sql               # CREATE PROCEDURE bodies
  views.sql
  functions.sql
artifacts/
  scoping-agent/
    {run_id}.input.json        # agent input committed before dispatch
    {run_id}.json              # agent output, immutable, committed by GHA or local agent
  profiler-agent/
    {run_id}.input.json
    {run_id}.json
  model-generator-agent/
    {run_id}.input.json
    {run_id}.json
  test-generator-agent/
    {run_id}.input.json
    {run_id}.json
dbt/
  models/                      # generated dbt models and schema files
```

Catalog files (`catalog/tables/`, `catalog/procedures/`) are the shared state between all execution paths. They are read and written by the deterministic Python skills.

---

## DDL Extraction

The `setup-ddl` CLI extracts DDL and builds catalog files from MCP query results. This runs once at project setup. Source metadata (technology, dialect, schemas) is recorded in `manifest.json` at the project root.

---

## Pipeline

Four stages, executed in order:

```text
Scoping ──► Profiling ──► Migration ──► Test Generation
```

Each stage reads upstream catalog/artifact data and produces its own output. See [Agent Contracts](../agent-contract/README.md) for per-agent input/output schemas and [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for skill contracts and shared library.

| Stage | Agent | What it does | Status |
|---|---|---|---|
| Scoping | `scoping-agent` | Discover writers for each table via catalog refs + AST fallback | Implemented |
| Profiling | `profiler-agent` | Classify tables, identify keys/watermarks/FKs/PII | Implemented |
| Migration | `model-generator-agent` | Generate dbt models from proc bodies + profile answers | Implemented |
| Test Generation | `test-generator-agent` | Generate schema tests and unit test fixtures | **Not yet implemented** |

---

## Interactive Migration

The FDE uses Claude Code skills directly. The `/migrate-table` orchestrator command drives the full pipeline for a single table with approval gates at every step.

Flow:

1. `/discover-objects` -- list tables, pick one
2. `/discover-objects show` -- statement breakdown, resolve `claude` statements via LLM + FDE confirmation
3. `/profile-table` -- catalog signals + LLM inference, FDE approves candidates
4. `/generate-model` -- generate dbt model, FDE approves before file write

Each step reads from and writes to catalog files in the migration repo. The FDE reviews and edits before approving. See [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for full skill contracts.

---

## Batch Execution

> **Not yet implemented.** The `migrate-util` CLI and GHA dispatch workflows described below are planned but not built.

### Local Batch

```bash
migrate-util scope --table dbo.FactSales
migrate-util profile --table dbo.FactSales
migrate-util migrate --table dbo.FactSales
```

Runs the agent pipeline for a single table locally. No approval gates -- the agent runs autonomously and commits results to the migration repo.

### GHA Dispatch

```bash
migrate-util dispatch scope --all
migrate-util dispatch profile --all
```

Commits an `{action}/{run_id}.input.json` to the migration repo and triggers a `workflow_dispatch` on the corresponding GHA workflow. The workflow:

1. Clones the migration repo.
2. Installs genai-toolbox binary; starts DDL file MCP in HTTP mode on `localhost:5000`.
3. Installs Claude Code CLI with the `ad-migration` plugin.
4. Runs the agent.
5. Creates branch `run/{run_id}`, commits output JSON, merges into `main`, deletes the run branch.

---

## Status

> **Not yet implemented.** Depends on the `migrate-util` CLI.

Status is derived entirely from artifact JSONs in the migration repo. No SQLite, no separate status table.

```bash
migrate-util status
migrate-util status --table dbo.FactSales
```

For each table, `status` scans `artifacts/{action}/` directories, finds the latest run per table per stage, and reports:

| Field | Source |
|---|---|
| Stage completion | Presence of `{run_id}.json` output file |
| Success/failure | `status` field in the output JSON |
| Staleness | Compare upstream artifact timestamps (a profile run older than its scope input is stale) |

Last run per table wins -- a later run supersedes all prior runs for that table.

---

## Agent Execution Model

> **GHA dispatch not yet implemented.** The per-agent workflow design below is planned.

One GitHub Actions workflow file per agent. Each workflow run corresponds to exactly one agent and one batch of tables.

### Agent Plugin

The repo root is a Claude Code marketplace package containing three plugins (bootstrap, migration, ground-truth-harness). Plugin structure, skill contracts, and local dev setup: [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md).

### Workflow Steps (planned)

1. Clone the migration repo.
2. Install genai-toolbox binary; start DDL file MCP in HTTP mode on `localhost:5000`.
3. Install Claude Code CLI.
4. Run agent via the `ad-migration` plugin.
5. Create branch `run/{run_id}`.
6. Commit output JSON to `artifacts/{action}/{run_id}.json` on that branch.
7. Merge `run/{run_id}` into `main` (each run touches a unique file path -- no conflicts).
8. Delete the `run/{run_id}` branch.
