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

## Environment Setup

Four commands prepare a migration repo before any per-table work begins. Steps 1-3 run once per project; step 4 runs once per test-generation batch.

| Step | Command | Type | Prerequisites | Produces |
|---|---|---|---|---|
| 1. Scaffold project | `/init-ad-migration` | Plugin command | Plugin loaded, uv, Python 3.11+ | `CLAUDE.md`, `README.md`, `repo-map.json`, `.gitignore`, `.githooks/` |
| 2. Extract DDL + catalog | `/setup-ddl` | Skill (interactive) | toolbox on PATH, MSSQL env vars (`MSSQL_HOST`, `MSSQL_PORT`, `SA_PASSWORD`) | `manifest.json`, `ddl/*.sql`, `catalog/**/*.json` |
| 3. Scaffold dbt project | `/init-dbt` | Plugin command | `manifest.json`, populated `catalog/tables/` | `dbt/` project tree with `sources.yml` |
| 4. Create test sandbox | `test-harness sandbox-up` | CLI (`uv run`) | `manifest.json`, MSSQL env vars | `__test_<run_id>` throwaway database |

### Prerequisites

1. **GitHub account** with `gh` CLI authenticated (`gh auth login`).
2. **Claude Code CLI** installed and authenticated.
3. **`ad-migration` plugin** installed (marketplace package containing bootstrap, migration, and ground-truth-harness plugins).

### Step details

**`/init-ad-migration`** — checks prerequisites (uv, Python, toolbox, MSSQL vars, git, direnv), presents a plan, then scaffolds project files and configures git hooks. Entry point for every new migration project.

**`/setup-ddl`** — connects to a live SQL Server via MCP, extracts DDL for user-selected databases and schemas, builds catalog files with 12 signal queries (PKs, FKs, identity, CDC, change tracking, sensitivity, DMF refs), and runs AST enrichment. All downstream stages depend on the catalog this produces.

**`/init-dbt`** — reads `manifest.json` and catalog to scaffold a dbt project with adapter-specific `profiles.yml` and `sources.yml` generated from catalog tables. User picks target platform (Fabric Lakehouse, Spark, Snowflake, DuckDB). Idempotent — regenerates `sources.yml` on re-run, never overwrites `profiles.yml`.

**`test-harness sandbox-up`** — creates a throwaway database (`__test_<run_id>`) by cloning schema and procedures from the source SQL Server. Used by the test generator to execute procs and capture ground truth. Torn down after test generation via `test-harness sandbox-down`.

---

## Migration Workflow

Six stages per table, with two quality-gate review loops:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Per-Table Pipeline                                 │
│                                                                             │
│  Scoping ──► Profiling ──► Sandbox Up ──►┌──────────────┐──► Migration Loop │
│                                          │  Test Loop   │                   │
│                                          │              │                   │
│                                          │  Generator   │    ┌────────────┐ │
│                                          │    ▼         │    │ Model Gen  │ │
│                                          │  Reviewer    │    │    ▼       │ │
│                                          │    │         │    │ dbt test   │ │
│                                          │  pass? ──no──│─┐  │ (≤3 iter) │ │
│                                          │    │    (≤2) │ │  │    ▼       │ │
│                                          │   yes        │ │  │ Code Rev  │ │
│                                          └────┼─────────┘ │  │    │      │ │
│                                               │           │  │  pass?    │ │
│                                               ▼           │  │  no ──────│─┤
│                                          test-specs/      │  │  (≤2)    │ │
│                                               │           │  │   yes     │ │
│                                               └───────────│──►   ▼       │ │
│                                                           │  │  Done     │ │
│                                                           │  └────────────┘ │
│                                                           │                 │
│                                                    ◄──────┘  ──► Sandbox   │
│                                                                   Down     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Stages

| Stage | Executor | Role |
|---|---|---|
| Scoping | `scoping-agent` | Discover which stored procedure writes each table. Reads catalog refs, falls back to AST analysis. Writes `selected_writer` to catalog. |
| Profiling | `profiler-agent` | Classify table, identify keys, watermark, FKs, PII. Writes profile answers to catalog. |
| Sandbox Up | `test-harness sandbox-up` (CLI) | Create throwaway database for ground-truth capture. Not an agent. |
| Test Generation | `test-generator-agent` + `test-reviewer-agent` | Generator enumerates proc branches, synthesizes fixtures, executes proc in sandbox, captures ground truth, writes `test-specs/<item_id>.json`. Reviewer independently enumerates branches, scores coverage, reviews fixture quality. Kicks back for missing branches or quality issues. **Max 2 review iterations.** |
| Migration | `model-generator-agent` + `code-reviewer-agent` | Model generator reads profile + test spec, generates dbt model + schema YAML (with `unit_tests:` rendered from test spec), runs `dbt test`, self-corrects up to **3 iterations**. Code reviewer checks standards, correctness, test integration. Kicks back for issues. **Max 2 review iterations.** |
| Sandbox Down | `test-harness sandbox-down` (CLI) | Drop throwaway database. Idempotent. |

**Key design decisions:**

- Test generation runs BEFORE migration — the model-generator consumes the approved test spec and must pass `dbt test` against it.
- Sandbox is a CLI step, not an agent — it's deterministic infrastructure.
- Review agents are pure quality gates — they don't generate artifacts or modify files.

Full per-agent contracts (input/output schemas, pipeline steps, boundary rules): [Agent Contracts](../agent-contract/README.md).

---

## Migration Repository

One project per repo. The migration repo is shared state between all execution paths — catalog files are the source of truth that every stage reads and writes.

### Core Layout

```text
manifest.json                       # source metadata (technology, dialect, database, schemas)
catalog/                            # shared state — all stages read/write here
  tables/
    <schema>.<table>.json           # columns, keys, FKs, PII, profile answers, scoping results
  procedures/
    <schema>.<proc>.json            # params, references, resolved statements
  views/
    <schema>.<view>.json
  functions/
    <schema>.<func>.json
ddl/                                # extracted DDL — read-only after setup-ddl
  tables.sql
  procedures.sql
  views.sql
  functions.sql
test-specs/                         # test-generator output → model-generator input
  <item_id>.json                    # branch manifest, unit_tests[], ground truth
dbt/                                # generated dbt project
  models/
    staging/
      sources.yml                   # generated by init-dbt from catalog
    ...
```

### Batch Artifacts

The `artifacts/` directory exists only for batch and GHA execution paths. It is the audit trail for agent runs — the interactive path does not use it.

```text
artifacts/
  <agent-name>/
    <run_id>.input.json             # committed before dispatch (what was requested)
    <run_id>.json                   # committed after completion (what was produced)
```

Each agent gets its own subdirectory. Input is committed before the agent starts; output is committed after it finishes. Run files are append-only — a later run supersedes prior runs for the same table. The interactive path writes directly to catalog and test-specs, skipping artifacts entirely.

---

## Execution Paths

Three paths, one pipeline. All share the same deterministic Python CLIs and agent contracts — the difference is orchestration.

```text
┌─────────────────────────────────────────────────────────┐
│                   Migration Repository                  │
│        (1 project = 1 repo, all state in catalog)       │
└────────────────────────┬────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   Interactive      Local Batch       GHA Batch
   (skills +        (migrate-util     (migrate-util
    approval)        --table)          dispatch --all)
        │                │                │
        ▼                ▼                ▼
   FDE approves      Autonomous       Autonomous
   every step        single-table     full-scope
```

| Path | Entry point | Approval gates | Runs where | Status |
|---|---|---|---|---|
| Interactive | Claude Code skills | Yes — every step | Local terminal | Implemented |
| Local batch | `migrate-util <stage> --table X` | None — agent runs autonomously | Local terminal | **Not yet implemented** |
| GHA batch | `migrate-util dispatch <stage> --all` | None — commits input, triggers workflow | GitHub Actions | **Not yet implemented** |

### Interactive Flow

The FDE drives the pipeline one table at a time using Claude Code skills:

1. `/discover-objects` — list tables, pick one, resolve writer
2. `/profile-table` — catalog signals + LLM inference, FDE approves
3. `/generate-tests` — branch analysis, fixture synthesis, sandbox execution, ground truth capture
4. `/generate-model` — generate dbt model from profile + test spec, FDE approves before file write

Each step reads from and writes to catalog files. The FDE reviews and edits before approving.

### Batch Execution

> **Not yet implemented.** The `migrate-util` CLI and GHA dispatch workflows are planned but not built.

**Local batch:** runs the agent pipeline for a single table locally, autonomously committing results.

**GHA batch:** commits `artifacts/<agent>/<run_id>.input.json`, triggers a `workflow_dispatch` on the corresponding GHA workflow. The workflow clones the repo, starts the MCP server, runs the agent, and commits output.

---

## Status

> **Not yet implemented.** Depends on the `migrate-util` CLI.

Status is derived from catalog files and batch artifact JSONs. No SQLite, no separate status table.

For each table, `status` finds the latest run per stage and reports completion, success/failure (`status` field), and staleness (upstream artifact newer than downstream).
