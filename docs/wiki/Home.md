# Migration Utility

A Claude Code plugin and batch CLI pipeline that migrates Microsoft Fabric Warehouse stored procedures to dbt models. Targets silver and gold transformations only (bronze is out of scope).

## Who is this for?

- **Field Data Engineers (FDEs)** running customer migrations
- **Customers** doing self-service stored-procedure-to-dbt conversions

## Pipeline Overview

The migration pipeline has two phases: **environment setup** (run once per project) and **per-table migration** (run for each table).

### Environment Setup

| Step | Command | What it does |
|---|---|---|
| 1 | `/init-ad-migration` | Scaffold project files, check prerequisites |
| 2 | `/setup-ddl` | Extract DDL and build catalog from live SQL Server |
| 3 | `/init-dbt` | Scaffold dbt project with sources from catalog |
| 4 | `/setup-sandbox` | Create throwaway test database for ground-truth capture |

### Per-Table Migration

```text
Scoping (/scope)
    │
    ▼
Profiling (/profile)
    │
    ▼
Test Generation (/generate-tests)
  ┌─────────────────────────┐
  │ generate → review → fix │  ≤2 review iterations
  └─────────────────────────┘
    │
    ▼
Model Generation (/generate-model)
  ┌─────────────────────────┐
  │ generate → dbt test     │  ≤3 self-corrections
  │ → code review → fix     │  ≤2 review iterations
  └─────────────────────────┘
    │
    ▼
  Done
```

## Two Ways to Run

| Mode | Entry point | Tables | Approval |
|---|---|---|---|
| Interactive | Skills (`/scoping-table`, `/profiling-table`, etc.) | One at a time | FDE reviews each step inline |
| Multi-table | Commands (`/scope`, `/profile`, etc.) | Multiple in parallel | FDE reviews summary at end |

**Example (multi-table):**

```text
/scope silver.DimCustomer silver.DimProduct silver.FactSales
```

## Where to Start

- **New to the tool?** Start with [[Installation and Prerequisites]], then follow the [[Quickstart]]
- **Already set up?** Jump to the stage you need from the sidebar
- **Looking up a specific skill?** See the Skill Reference section in the sidebar
- **Troubleshooting?** See [[Troubleshooting and Error Codes]]

## Repository Layout

A migration project produces this directory structure:

```text
manifest.json                 # source metadata (technology, dialect, database)
catalog/                      # shared state — all stages read/write here
  tables/<schema>.<table>.json
  procedures/<schema>.<proc>.json
  views/<schema>.<view>.json
  functions/<schema>.<func>.json
ddl/                          # extracted DDL (read-only after setup)
  tables.sql, procedures.sql, views.sql, functions.sql
test-specs/                   # test fixtures (test-gen output → model-gen input)
  <item_id>.json
dbt/                          # generated dbt project
  models/staging/sources.yml
  ...
```

The catalog is the source of truth. Every stage reads from and writes to catalog files. Git is the durable store — catalog files, test specs, and dbt models are committed artifacts.
