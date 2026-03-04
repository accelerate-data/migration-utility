# Migration Utility

*Read top to bottom before touching any linked document.*

---

## The Problem

Vibedata is built for greenfield. The world is brownfield.

Customers arrive with existing silver and gold transformation logic — T-SQL stored procedures in SQL Server databases. Until those are on Vibedata standards (dbt models, unit tests, CI/CD, lineage), none of our agents can help them. The Migration Utility is what gets them there.

Without it, migration is a manual FDE engagement measured in weeks per domain. That pace caps how many customers can onboard and how quickly they see value from the platform.

---

## What It Does

The utility migrates SQL Server stored procedures to dbt models following Vibedata standards — one domain at a time, ready for the FDE to merge into the production repo.

**Setup (Tauri desktop app).** The FDE creates a project by uploading a DacPac and configuring the migration repo. State persists to local SQLite so sessions can span multiple days around domain owner conversations.

**Execution (GitHub Actions + Claude Agent SDK).** Execution runs as six gated stages: Scope, Profile, Decompose, Plan, Generate Tests, and Migrate. Each stage runs headless in GitHub Actions via `workflow_dispatch`, writes structured JSON artifacts to the migration repo, and pauses for FDE review before the next stage.

When all Migrate-tier procedures pass, the utility pushes a branch to the production repo. The FDE opens a standard PR. UAT runs in CI; once signed off, merge completes the cutover.

---

## FDE User Journey

The FDE works through these surfaces in the Tauri app. Full screen-level detail is in [docs/design/ui-patterns/README.md](../design/ui-patterns/README.md).

### Surfaces

| Surface | Purpose |
|---------|---------|
| **Splash** | Prerequisite checks (Docker Desktop). Blocks until Docker is available. |
| **Settings** | GitHub OAuth, migration repo path, local clone path. |
| **Projects** | Create, select active, archive, delete projects. Each project is a DacPac + metadata. |
| **Stage tabs** | Six tabs (Scope → Profile → Decompose → Plan → Generate Tests → Migrate). Each tab: table list, status filter, FDE review modal, Submit button. |

### Key journeys

**First launch:** Splash screen checks Docker Desktop. After Docker is confirmed, the app checks GitHub auth. If not set up, the FDE logs in via OAuth and selects an empty migration repo.

**Project creation:** FDE creates a project by entering a name, SQL Server version, DacPac path, SA password, and source metadata. The app pushes the DacPac to the migration repo via Git LFS and starts a local Docker SQL Server container.

**Execution:** FDE works through the six stage tabs in order:

1. Scope — select tables and submit to the analysis agent.
2. Profile — review scope output, submit to the profiler agent.
3. Decompose — review profile output, submit to the decomposer agent.
4. Plan — review decompose output, submit to the planner agent.
5. Generate Tests — review plan output (read-only), submit to the test generator agent.
6. Migrate — review plan + test output (read-only), submit to the migrator agent.

Each tab has a Refresh button that pulls the repo and re-consolidates status. FDE can edit agent output for stages 1–4 before submitting the next stage.

**Session resumption:** SQLite owns local app state; JSON artifacts in the migration repo own execution state. Both survive process restart.

### UI constraints

| Constraint | Requirement |
|------------|-------------|
| One active project at a time — the entire UI operates on the selected project | DEC-04 |
| Table-first scope selection — stored procedure discovery is automatic | DEC-10 |
| Stage tabs are funnel-gated — a table appears in tab N only if it succeeded in tab N-1 | DEC-19 |
| FDE overrides on stages 1–4 mark the downstream stage dirty | DEC-11, DEC-19 |

---

## Key Design Decisions

### Migration repo

One migration repo per installation, set by the user in Settings. All projects share a single repo. Agent output (JSON artifacts) is committed to the repo by GitHub Actions. DacPac files are stored via Git LFS. The production repo is never touched during migration — when all Migrate-tier procedures pass, the utility pushes a branch to the production repo. The FDE opens a standard PR.

### Stage artifacts and approvals

Each agent run produces an immutable JSON artifact committed to `{project-slug}/artifacts/{action}/{run_id}.json`. These are the canonical execution state for all six stages. FDE overrides are stored in local SQLite only — agent output in git is never modified. The effective input to the next agent is `COALESCE(fde_value, agent_value)`.

### Data sampling

A point-in-time snapshot is taken of every table in the dependency graph (bronze through gold). Dimensions are copied in full. Facts are sampled to one day on the incremental column (most recent complete day, FDE-overridable). Downstream silver/gold rows are filtered to only those derivable from the 1-day fact sample. PII columns are masked before fixture generation.

### Unit testing

Each agent-migrated model gets dbt unit tests and YAML fixtures generated as part of migration. Fixtures come from the production snapshot. Both `dbt test --select test_type:unit` and `dbt build` must pass before the PR is raised. Tests are permanent artifacts that carry into the production repo, required to pass Vibedata's CI coverage gates.

### UAT and data validation

UAT is Vibedata's standard CI/CD — the utility doesn't own it. When the FDE opens the PR, CI runs the full E2E pipeline against production sources as a parallel run alongside the legacy pipelines. The domain owner signs off on the output comparison before merge.

---

## Document Index

| Document | Purpose |
|----------|---------|
| [decisions.md](decisions.md) | Binding architectural decisions (DEC-01 through DEC-20). Read before changing any architectural boundary. |
| [build-plan.md](build-plan.md) | Week-by-week plan, mock scenario catalogue, risk register. |
| [../design/ui-patterns/README.md](../design/ui-patterns/README.md) | Surfaces, user flows, screen patterns, and interactive mockup. |
| [research/vibedata-architecture.md](research/vibedata-architecture.md) | Vibedata platform architecture: modules, agents, deployment, CI/CD. |
| [research/vibedata-strategy.md](research/vibedata-strategy.md) | Problem statement, personas, differentiation, metrics. |
| [research/](research/) | dbt unit test strategy, domain memory management, competitor analysis. |
