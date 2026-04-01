# Migration Utility — Decisions

*Last updated: 2026-03-04. Decisions are grouped by reasoning chain — later decisions in each section depend on earlier ones.*

---

## 1. Scope and Target Stack

*Load-bearing constraints. Everything else is derived from them.*

### DEC-01 — Transformations Only

**Decision:** Silver and gold transformations only. Bronze (DLT/mirroring) is out of scope — handled by a separate utility.
**Rationale:** Bronze has a different migration path. Mixing concerns complicates both utilities.

---

### DEC-02 — Source: SQL Server via DacPac

**Decision:** The utility ingests SQL Server databases via DacPac files. The FDE provides a DacPac extracted from the source system. Lakehouse (Spark notebooks/PySpark) source support is post-MVP.
**Rationale:** DacPac captures the full schema and stored procedure definitions in a portable artifact. Lakehouse notebooks require SQL extraction from PySpark — a harder problem, deferred.

---

### DEC-03 — Input: DacPac + Migration Repo

**Decision:** The utility takes a DacPac file as input and stores all work in a single migration repo on GitHub. One migration repo per installation; all projects share it. Production artifacts are never modified.
**Rationale:** A single repo simplifies GitHub Actions configuration, LFS setup, and cross-project artifact management.

---

## 2. People and Process Boundaries

*Who does what, when, and with what authority.*

### DEC-04 — Migration is Per-Domain

**Decision:** Migration runs one domain at a time, not in bulk. The FDE performs the migration; the domain owner signs off scope only.
**Rationale:** Per-domain limits blast radius and keeps accountability with the domain owner.

---

## 3. Repo Architecture and CI/CD

*What gets created and what gets replaced.*

### DEC-05 — Production Repo to Vibedata Standards

**Decision:** A new production GitHub repo is created following Vibedata standards. This is separate from the migration repo where the utility does its work. We do not retrofit the customer's existing repo.

---

### DEC-06 — CI/CD: Replace, Not Extend

**Decision:** Customers adopt Vibedata's CI/CD and branch model. We do not extend or integrate with their existing workflows.

---

### DEC-07 — Observability: Automatic

**Decision:** No observability setup during migration. It activates automatically when the domain runs on Vibedata's standard pipelines.

---

### DEC-08 — Handoff: Utility Pushes to Production Repo Branch

**Decision:** Utility pushes completed code to a branch in the production repo. FDE opens a standard PR from there. The PR, UAT, and merge all happen in the production repo using standard Vibedata CI/CD.

---

### DEC-09 — Cutover and Legacy Retirement

**Decision:** Cutover happens on PR merge via standard Vibedata CI/CD. Legacy pipeline retirement is the domain owner's responsibility and outside the utility's scope.

---

## 4. Source Artifact and Infrastructure

*How the source database is captured, stored, and made available to agents.*

### DEC-10 — DacPac via Git LFS

**Decision:** The DacPac file is stored in the migration repo via Git LFS. The app enables LFS on the repo at project creation before the first DacPac push. `metadata.json` (customer, system, db name, extraction datetime, tool version, DacPac SHA256) is committed alongside it.
**Rationale:** Git LFS handles large binary storage natively. SHA256 keying enables deterministic caching in GitHub Actions.

---

### DEC-11 — Local Docker SQL Server for DacPac Inspection

**Decision:** The app manages a local Docker SQL Server container. The DacPac is restored to this container at project initialization. Agents running in GitHub Actions also restore the DacPac to a SQL Server container (with MDF/LDF caching keyed on DacPac SHA256).
**Rationale:** DacPac inspection requires a running SQL Server instance. Docker provides a disposable, reproducible environment both locally and in CI.

---

### DEC-12 — GitHub OAuth Authentication

> **Superseded.** This decision described Tauri desktop app auth. The project is now a CLI tool using `gh` CLI authentication.

~~**Decision:** The app uses a classic OAuth App with `repo` and `workflow` scopes. Token is stored in local SQLite with silent refresh via OAuth refresh token flow.~~

---

## 5. Pipeline Stages

*Six-stage pipeline. Each stage runs as a GitHub Actions workflow per agent.*

### DEC-13 — Six-Stage Pipeline

**Decision:** Migration runs as six sequential stages:

1. **Scope** (`scoping-agent`) — identifies candidate writers for each table.
2. **Profile** (`profiler-agent`) — classifies tables; identifies keys, watermarks, FKs, PII.
3. **Migrate** (`model-generator-agent`) — translates source SQL to dbt models, derives materialization, generates schema tests.
4. **Test Generation** (`test-generator-agent`) — generates dbt unit tests and YAML fixtures. *Not yet implemented.*

Each stage produces immutable JSON artifacts committed to the migration repo by the GitHub Actions runner.

**Rationale:** Six stages provide fine-grained FDE intervention points and enable selective re-runs without restarting the full pipeline.

---

### DEC-14 — Table-First Scope Selection

**Decision:** FDE selects domain tables first. The analysis agent traces each table back to the stored procedure that produces it (via write targets: `MERGE INTO`, `INSERT INTO`, `CREATE TABLE AS SELECT`). Candidacy runs only on the resolved artifacts.
**Rationale:** Domain owners think in tables, not code artifacts. Table-first catches orphan tables, duplicate writers, and cross-domain dependencies early.

---

### DEC-15 — FDE Overrides: First Four Stages Only

**Decision:** FDE can edit agent outputs for Scope, Profile, Decompose, and Plan. Generate Tests and Migrate outputs are read-only final artifacts — no FDE overrides are permitted.
**Rationale:** The first four stages shape what gets migrated and how. The last two produce deterministic outputs of the plan — editing them would bypass the audit trail.

---

### DEC-16 — Backwards Dirty Computation

**Decision:** Status consolidation runs a backwards pass from Migrate to Profile. A stage is dirty if: (1) its own last run failed, (2) an FDE override exists on its upstream stage, or (3) its upstream artifact is newer than its own. Scope has no upstream and is never dirty. No propagation pass is needed — each re-run produces a new artifact timestamp, and the next consolidation naturally marks the downstream stage dirty.
**Rationale:** Backwards-only computation avoids cascading re-runs. The FDE works forward from `dirty_from` naturally.

---

## 6. Execution

*GitHub Actions for all agent execution. The desktop app is the control plane.*

### DEC-17 — Execution Runtime: GitHub Actions via workflow_dispatch

> **Partially superseded.** The Tauri desktop app is no longer planned. The interactive path uses Claude Code skills directly. The GHA batch path is planned but not yet implemented.

**Decision:**

- **Interactive path:** FDE uses Claude Code skills (`/listing-objects`, `/scoping-table`, `/profiling-table`, `/generating-model`) with approval gates at every step.
- **Agent execution (headless, planned):** one GitHub Actions workflow file per agent. A `migrate-util` CLI triggers runs via `workflow_dispatch`.

**Rationale:** GitHub Actions eliminates hosting infrastructure. `workflow_dispatch` gives deterministic control over what runs when. Branch-per-run avoids merge conflicts (unique file paths).

---

### DEC-18 — dbt-core-mcp for Lineage, Compilation, and Validation

**Decision:**

- Agents use [dbt-core-mcp](https://github.com/NiclasOlofsson/dbt-core-mcp) as an MCP server for all dbt interactions: lineage resolution, compiled SQL retrieval, model execution, and validation queries.
- Column-level lineage (`get_column_lineage`) used for transformation validation.
- Requires dbt Core 1.9+.

**Rationale:** Eliminates custom dbt tooling. Lineage, compilation, and execution become MCP calls instead of shell wrappers.

---

### DEC-19 — Review/Reject: Parallel Manual FDE Track

**Decision:** Stored procedures that the agent cannot handle are flagged. The FDE migrates these manually in parallel with automated migration. Both tracks converge at the production repo PR.

---

## 7. Testing

*Agents generate dbt unit tests with YAML fixtures. Fixture data comes from a production snapshot.*

### DEC-20 — Testing Approach: dbt Unit Tests with YAML Fixtures

**Decision:**

- The test-generator agent produces dbt unit tests and YAML fixtures for every migrated model.
- Tests and fixtures are committed as permanent artifacts that carry into the production repo.
- All testing runs on a dedicated persistent Fabric workspace using `dbt-fabricspark` (F2 PAYG with pause/resume automation).
- `dbt test --select test_type:unit` and `dbt build` must pass before the PR is raised.
- FDE reviews at PR time.

**Rationale:** Models without unit tests fail Vibedata's CI coverage gates.

---

### DEC-21 — Fixture Data Source: Point-in-Time Snapshot

**Decision:**

- Fixtures are derived from a point-in-time snapshot of all tables in the dependency graph — bronze, silver, and gold.
- All migration work (code generation, fixture generation) runs against this snapshot exclusively.
- After migration, dbt source definitions are updated to point to live production tables.

**Rationale:** Snapshotting bronze only leaves intermediate silver/gold as moving targets, making fixture data inconsistent.

---

### DEC-22 — Snapshot Strategy by Table Type

**All tables:**

- PII columns are recommended by the agent per table, confirmed by the FDE, and masked before fixture generation.

**Dimensions:**

- Snapshotted in full.

**Facts:**

- Sampled to 1 day on the incremental column — most recent complete day by default, FDE-overridable.
- Incremental column is inferred from SQL patterns and confirmed by the FDE before snapshot runs.

**Downstream silver/gold:**

- Filtered to rows derivable from the 1-day fact sample.

**Full-refresh tables (no incremental pattern):**

- Copied in full and flagged to the FDE.

---

## 8. Validation

*Standard Vibedata CI/CD — the utility does not own it.*

### DEC-23 — UAT: Parallel Run in Ephemeral Workspace

**Decision:** After the FDE opens the PR, Vibedata's CI/CD provisions an ephemeral Fabric workspace. E2E pipelines run against live production sources as a parallel run alongside legacy pipelines. The domain owner signs off.

---

## 9. Memory and Rules (Optional)

*Fully decoupled from the migration utility. Optional step after migration is complete.*

### DEC-24 — Memory Seeding: Separate Agent, Two Sources

**Decision:**

- Memory and rules creation is optional and handled by a separate agent — not part of the migration utility.
- Two sources feed memory: (1) Studio reads chat history from the migration session, (2) a separate utility reads the migrated code to infer business rules and transformation patterns.
- FDE and domain owner work with the agent to curate and commit memory/rules to the repo.
- Business rules and domain semantics → `.claude/rules` (always in build agent context). Technical patterns → domain memory store (retrieved dynamically per intent).
