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

**Interactive path (Claude Code plugin).** The FDE uses Claude Code skills (`/discover`, `/profile`, `/migrate`) with approval gates at every step. See [Overall Design](../design/overall-design/README.md).

**Batch path (planned).** A `migrate-util` CLI and GitHub Actions workflows for autonomous multi-table migration. Not yet implemented.

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
| [decisions.md](decisions.md) | Binding architectural decisions (DEC-01 through DEC-24). Read before changing any architectural boundary. |
| [research/vibedata-architecture.md](research/vibedata-architecture.md) | Vibedata platform architecture: modules, agents, deployment, CI/CD. |
| [research/vibedata-strategy.md](research/vibedata-strategy.md) | Problem statement, personas, differentiation, metrics. |
| [research/](research/) | dbt unit test strategy, domain memory management, competitor analysis. |
