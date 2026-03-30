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
3. **`ad-migration` plugin** installed in Claude Code (marketplace package containing bootstrap, migration, and test-generation plugins).

---

## High-Level Architecture

```text
Three execution paths, one pipeline:

┌─────────────────────────────────────────────────────────┐
│                   Migration Repository                   │
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

| Path | Entry point | Approval gates | Runs where |
|---|---|---|---|
| Interactive | FDE opens Claude Code, uses skills (`/discover`, `/profile`, `/migrate`) | Yes -- every step | Local terminal |
| Local batch | `migrate-util scope --table X` | None -- agent runs autonomously | Local terminal |
| GHA batch | `migrate-util dispatch scope --all` | None -- commits input, triggers GHA workflow | GitHub Actions |

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
  migrator-agent/
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

The `setup-ddl` skill extracts DDL and catalog signals from the source system. This runs once at project setup.

### SQL Server (DacPac)

A bundled .NET 8 tool (`dacpac-extractor`) uses `Microsoft.SqlServer.DacFx` to unpack the DacPac and script all objects:

```bash
dacpac-extractor {source-file} {output-dir}
```

Outputs: `tables.sql`, `procedures.sql`, `views.sql`, `functions.sql`, `indexes.sql`.

For catalog extraction (keys, FKs, referenced entities), the skill connects to a live SQL Server instance via `export_ddl.py --catalog` and writes JSON files to `catalog/`.

### Other Sources (zip)

The skill unzips the archive and normalizes the contents into the same `ddl/` structure.

### Source Metadata

`artifacts/ddl/metadata.json` records:

- `technology` -- source system type
- `source_filename` -- original filename
- `source_sha256` -- hash of the source file
- `extraction_datetime` -- when extraction occurred
- `tool_version` -- migration utility version

---

## Pipeline

Four stages, executed in order:

```text
Scoping ──► Profiling ──► Migration ──► Test Generation
```

Each stage reads upstream catalog/artifact data and produces its own output. See [Agent Contracts](../agent-contract/README.md) for per-agent input/output schemas and [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for skill contracts and shared library.

| Stage | Agent | What it does |
|---|---|---|
| Scoping | `scoping-agent` | Discover writers for each table via catalog refs + AST fallback |
| Profiling | `profiler-agent` | Classify tables, identify keys/watermarks/FKs/PII |
| Migration | `migrator-agent` | Generate dbt models from proc bodies + profile answers |
| Test Generation | `test-generator-agent` | Generate schema tests and unit test fixtures |

---

## Interactive Migration

The FDE uses Claude Code skills directly. The `/migrate-table` orchestrator command drives the full pipeline for a single table with approval gates at every step.

Flow:

1. `/discover` -- list tables, pick one
2. `/scope` -- find writers, confirm which procedure to migrate
3. `/discover show` -- statement breakdown, resolve `claude` statements via LLM + FDE confirmation
4. `/profile` -- catalog signals + LLM inference, FDE approves candidates
5. `/migrate` -- generate dbt model, FDE approves before file write
6. `/test-gen` -- generate schema.yml + unit test fixtures, FDE approves before file write
7. `/validate` -- compare outputs (skipped if no live DB)

Each step reads from and writes to catalog files in the migration repo. The FDE reviews and edits before approving. See [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md) for full skill contracts.

---

## Batch Execution

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

### Test Generator (SQL Server)

The test generator agent requires a live database for validation. On GHA:

1. Pulls source file from LFS.
2. Starts SQL Server Docker container; restores database from source file.
3. Starts live execution MCP in HTTP mode.
4. Runs the test generator agent.

---

## Status

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

One GitHub Actions workflow file per agent. Each workflow run corresponds to exactly one agent and one batch of tables.

### Workflow Inputs

| Input | Description |
|---|---|
| `run_id` | UUID generated at submission time |
| `submitted_ts` | ISO 8601 UTC timestamp |

### Agent Plugin

`ad-migration` is a Claude Code marketplace package containing three plugins under `workbench/`. Plugin structure, skill contracts, and local dev setup: [SP to dbt Migration Plugin](../sp-to-dbt-plugin/README.md).

### Workflow Steps

1. Clone the migration repo.
2. Install genai-toolbox binary; start DDL file MCP in HTTP mode on `localhost:5000`.
3. Install Claude Code CLI.
4. Run agent via the `ad-migration` plugin.
5. Create branch `run/{run_id}`.
6. Commit output JSON to `artifacts/{action}/{run_id}.json` on that branch.
7. Merge `run/{run_id}` into `main` (each run touches a unique file path -- no conflicts).
8. Delete the `run/{run_id}` branch.

---

## FDE Overrides

FDE overrides are direct edits to catalog files committed to git. There is no separate override table or schema. See [fde-overrides.md](fde-overrides.md) for details.
