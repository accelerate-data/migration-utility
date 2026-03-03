# Migration Utility

*Read top to bottom before touching any linked document.*

---

## The Problem

Vibedata is built for greenfield. The world is brownfield.

Customers arrive with existing silver and gold transformation logic — T-SQL stored procedures in Microsoft Fabric Warehouse, orchestrated by Azure Data Factory pipelines. Until those are on Vibedata standards (dbt models, unit tests, CI/CD, lineage), none of our agents can help them. The Migration Utility is what gets them there.

Without it, migration is a manual FDE engagement measured in weeks per domain. That pace caps how many customers can onboard and how quickly they see value from the platform.

---

## What It Does

The utility migrates a Fabric Warehouse to Lakehouse following Vibedata standards — one domain at a time, ready for the FDE to merge into the production repo.

**Setup (Tauri desktop app).** The FDE works through a wizard on their laptop. State persists to local SQLite so sessions can span multiple days around domain owner conversations.

**Execution (GitHub Actions + Claude Agent SDK).** Execution runs as gated batch stages: scoping, profiling, planning, and migration/testing. Each stage runs headless in GitHub Actions, writes structured JSON results, and pauses for FDE approval before the next stage.

When all Migrate-tier procedures pass, the utility pushes a branch to the production repo. The FDE opens a standard PR. UAT runs in CI via an ephemeral Fabric workspace; once signed off, merge completes the cutover.

---

## FDE User Journey

The FDE works through four surfaces in the Tauri app. Full screen-level detail is in [docs/design/ui-patterns/README.md](../design/ui-patterns/README.md).

### Surfaces

| Surface | Purpose |
|---------|---------|
| **Home** | Status at a glance. Routes the FDE to the right next step. Three states: Setup required / Ready (wizard progress) / Active (pipeline running). |
| **Scope** | Select in-scope tables and approve scoping candidates. Freely navigable before scope finalization; locked read-only after. |
| **Monitor** | Trigger each batch stage and track per-item status/results. |
| **Settings** | Connections (one-time) · Workspace (per-migration) · Reset · Usage (cost tracking). |

### Key journeys

**First launch:** Home shows "Setup required". FDE goes to Settings → Connections (GitHub + Anthropic API key), then Settings → Workspace (Fabric URL, SP credentials, migration repo, working directory). Once both are configured, Home shows Ready.

**Migration setup:** FDE opens Scope and selects domain tables. The utility traces each table back to one or more producing stored procedures (DEC-10) and surfaces scoping candidates for FDE confirmation (DEC-11).

**Execution:** FDE opens Monitor and runs the stages in order:

1. Trigger scoping batch and approve results.
2. Trigger profiling batch and approve results.
3. Trigger planning batch and approve results.
4. Trigger migration/testing batch.
5. After tests pass, open PR for approval.

**Session resumption:** SQLite owns setup state; stage JSON artifacts in the migration repo own execution state. Both survive process restart. Home restores to the correct state on reopen (DEC-19).

**Reset:** Settings → Reset clears the migration repo branch, local working directory, scope selections, and workspace config. GitHub and Anthropic credentials are kept.

### UI constraints

| Constraint | Requirement |
|------------|-------------|
| One migration at a time — Home shows one active migration, never a list | DEC-04 |
| Table-first scope selection — stored procedure discovery is automatic | DEC-10 |
| Workspace locked while a stage is running | DEC-19 |
| Scope locked (read-only) after scoping approval | DEC-19 |
| Next stage cannot run until current stage is FDE-approved | DEC-11, DEC-19 |

---

## Key Design Decisions

### Migration repo

All utility work happens in an isolated migration repo — the production repo is never touched during migration. When all Migrate-tier procedures pass, the utility pushes a branch to the production repo (created fresh to Vibedata standards). The FDE opens a standard PR. The migration repo is scaffolding; it doesn't ship.

### Stage artifacts and approvals

Each stage produces structured batch JSON artifacts (input/output with `items[]` and `results[]`). These are the canonical execution contract and state for scoping, profiling, planning, and migration/testing. Markdown views can be generated from JSON for readability, but JSON remains source of truth.

### Data sampling

A point-in-time snapshot is taken of every table in the dependency graph (bronze through gold). Dimensions are copied in full. Facts are sampled to one day on the incremental column (most recent complete day, FDE-overridable). Downstream silver/gold rows are filtered to only those derivable from the 1-day fact sample. PII columns are masked before fixture generation.

### Unit testing

Each agent-migrated model gets dbt unit tests and YAML fixtures generated as part of migration. Fixtures come from the production snapshot. Both `dbt test --select test_type:unit` and `dbt build` must pass before the PR is raised. Tests are permanent artifacts that carry into the production repo, required to pass Vibedata's CI coverage gates.

### UAT and data validation

UAT is Vibedata's standard CI/CD — the utility doesn't own it. When the FDE opens the PR, CI provisions an ephemeral Fabric workspace and runs the full E2E pipeline against live production sources as a parallel run alongside the legacy pipelines. The domain owner signs off on the output comparison before merge.

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
