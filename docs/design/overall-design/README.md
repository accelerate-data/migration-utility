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
│                                          │  pass? ──no──│─┐  │ (≤3 iter)  │ │
│                                          │    │    (≤2) │ │  │    ▼       │ │
│                                          │   yes        │ │  │ Code Rev   │ │
│                                          └────┼─────────┘ │  │    │       │ │
│                                               │           │  │  pass?     │ │
│                                               ▼           │  │  no ────── │─┤
│                                          test-specs/      │  │  (≤2)      │ │
│                                               │           │  │   yes      │ │
│                                               └───────────│──►   ▼        │ │
│                                                           │  │  Done      │ │
│                                                           │  └────────────┘ │
│                                                           │                 │
│                                                    ◄──────┘  ──► Sandbox    │
│                                                                   Down      │
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

### No artifacts directory

Agents write directly to catalog files, test-specs, and dbt models. There is no separate `artifacts/` directory for input/output JSON. The catalog IS the pipeline state. Run metadata (timing, cost, per-item status) is tracked in a transient `.migration-status.json` that is `.gitignore`d — it is consumed at commit/PR time to generate rich commit messages and PR bodies, then discarded.

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
   (skills +        (/batch-run       (migrate-util
    approval)        command)          dispatch --all)
        │                │                │
        ▼                ▼                ▼
   FDE approves      FDE present,     Autonomous
   every step        reviews at        full-scope
                     stage boundary
```

| Path | Entry point | Approval gates | Runs where | Status |
|---|---|---|---|---|
| Interactive | Claude Code skills | Yes — every step | Local terminal | Implemented |
| Local batch | `/batch-run <stage> --tables X,Y,Z` | Stage boundaries — FDE reviews between stages | Local terminal (Claude Code session) | **Not yet implemented** |
| GHA batch | `migrate-util dispatch <stage> --all` | None — fully autonomous | GitHub Actions | **Deferred** |

### Interactive Flow

The FDE drives the pipeline one table at a time using Claude Code skills:

1. `/discover-objects` — list tables, pick one, resolve writer
2. `/profile-table` — catalog signals + LLM inference, FDE approves
3. `/generate-tests` — branch analysis, fixture synthesis, sandbox execution, ground truth capture
4. `/generate-model` — generate dbt model from profile + test spec, FDE approves before file write

Each step reads from and writes to catalog files. The FDE reviews and edits before approving.

### Local Batch Execution

> **Not yet implemented.**

The FDE runs batches of 5-10 tables per stage from a Claude Code session using the `/batch-run` command. Two layers:

| Layer | What | How |
|---|---|---|
| **`migrate-util` CLI** (deterministic) | Item eligibility filtering, agent subprocess spawning, status tracking, git operations | Python Typer, no LLM, `lib/shared/migrate_util.py` |
| **`/batch-run` command** (interactive) | User-facing orchestration — task list, commit prompts, resolution questions, progress | Claude Code command, uses LLM for interaction only |

A "run" is scoped to one stage and one set of tables. Each run gets its own git branch:

```text
main
  ├── run/scope-batch-1          (scope tables 1-10)
  ├── run/profile-batch-1        (profile tables 1-10)
  └── run/migrate-batch-1        (migrate tables 1-5)
```

Multiple runs can be in flight: scope batch 2 while profiling batch 1 while migrating batch 0. Full E2E (`/batch-run run --table X`) chains all stages for a single table only.

Within a stage, the agent is autonomous (skip-and-continue on errors). Between stages, the FDE reviews via git diff, resolves ambiguous items, and decides whether to commit and proceed.

**Commit and PR strategy:** The `/batch-run` command asks the FDE whether to commit. When committing, the CLI reads `.migration-status.json` and generates a rich commit message with run summary (per-table status, timing, cost). When creating a PR, the same summary goes into the PR body. No summary files are committed — the transient status file is consumed at commit/PR time and discarded.

### GHA Batch Execution

> **Deferred.** Not in scope for the current implementation.

---

## Status

> **Not yet implemented.** Depends on the `migrate-util` CLI.

Status is derived entirely from catalog files. No SQLite, no separate status table, no `artifacts/` directory.

`migrate-util status` scans catalog files and test-specs to determine per-table stage completion:

| Field | Source |
|---|---|
| Scoping done | `catalog/tables/<table>.json` has `scoping` section |
| Profiling done | `catalog/tables/<table>.json` has `profile` section |
| Test-gen done | `test-specs/<table>.json` exists |
| Migration done | `dbt/models/` contains model `.sql` + `.yml` for the table |

The transient `.migration-status.json` adds in-flight progress (timing, cost, per-item status) during a batch run. The `rich` library renders a formatted status table to the terminal.
