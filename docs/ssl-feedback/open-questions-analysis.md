# Design Decisions

All design decisions for the showcase migration pipeline have been resolved. This document records each decision with its rationale.

---

## Plugin Purpose

**Decision**: This is a showcase migration tool, not a full warehouse migration tool. It minimizes the effort required to bring 2-3 fact tables and all their dimensional dependencies to Fabric Lakehouse so vibeData can be showcased on real, correctly-migrated data.

**Rationale**: The original design (whole-DB triage, domain classification, foundation layer detection, inter-domain ordering) was the wrong abstraction. The actual use case is always "bring these specific facts over." Starting from known targets and working backwards eliminates most of the complexity.

---

## Flow: Reverse Dependency Resolution

**Decision**: User specifies 2-3 target fact tables. The agent traces the full dependency tree backwards and migrates everything needed for those facts to produce exactly the same output as the source system.

**Rationale**: The dependency tree IS the scope. No triage, no domains, no foundation layer. dim_date gets pulled in if a fact needs it — there's no classification question. The scope is small (10-15 objects) and fully determined by the targets.

---

## Dependency Depth

**Decision**: Trace the full transitive closure of data dependencies from target facts. If removing a table from the procedure's logic changes the output rows of the target fact, it is a real dependency. If it only affects logging, error handling, or orchestration metadata, it is ETL plumbing and excluded.

**Rationale**: The showcase must produce exactly the same output as the source system. Anything less undermines the credibility of the migration and the vibeData demo.

---

## Migrate vs. Seed vs. Source

**Decision**:
- **dbt seed**: Static reference data with no writer procedure (dim_date, dim_currency). Load once from source, ship as CSV.
- **dbt source**: Staging/raw tables that are leaf nodes in the dependency tree (stg_customer, stg_sales). Bronze layer, out of scope for transformation.
- **Full migration**: Everything else — dimensions and facts with writer procedures containing business logic.

**Rationale**: Seeds are the pragmatic answer for static data. Since this is foundation quality, seeded dims are explicitly marked for upgrade to full models when moving from showcase to production migration.

---

## Test Coverage

**Decision**: Full branch-covering test generation with ground truth capture.

**Rationale**: These models are foundation quality — they become the starting point for the customer's actual migration. Schema-only tests (not_null, unique, relationships) are the minimum; ground truth tests validate exact output equivalence. The scope is small (10-15 objects), so thorough testing is affordable.

---

## Foundation Quality

**Decision**: Showcase models are the starting point for production migration, not throwaway demos.

**Implications**:
- Full test coverage
- Correct migration of business logic
- Seeded dims marked as "upgrade to full model for production"
- dbt project structure follows production conventions (staging/marts separation, schema tests, documentation)

---

## SCD2 / Multi-Table Writer Handling

**Decision**: Agent detects and summarizes using structural signals; human gates the decision.

**Detection signals (all naming-agnostic)**:
- Shared business key between two written tables (same natural key columns, excluding surrogates)
- Column type superset: one table has 2+ date/datetime columns and 1 boolean/bit column not present in the other
- Behavioral pattern: procedure UPDATEs one table and INSERTs into the other using the same key

**Rationale**: The showcase demonstrates agentic data engineering — the agent should show its reasoning. But at 10-15 objects, human review of 1-2 flagged items takes minutes. Agent detects, human gates.

---

## Naming-Agnostic Detection

**Decision**: All detection algorithms use structural and behavioral signals only. No hard-coded naming patterns (no `dim_date`, `valid_from`, `entity_history`). Names appear as supporting evidence for human reviewers, never as gate conditions.

**Rationale**: Names are opinions, structure is physics. Different data warehouses use different naming conventions. Structural signals (column types, FK relationships, SQL behavioral patterns) work regardless of naming.

---

## Self-Checks Between Pipeline Steps

**Decision**: Deterministic self-checks replace human approval gates. LLM reasoning is never used to validate LLM reasoning.

| Step | Hard Gates | Soft Warnings | Retries |
|---|---|---|---|
| Scope | Writer target matches table (SQL parse). Reference graph matches catalog. | — | N=1 |
| Profile | Classification matches structural patterns (type-based). | Key confidence < 80% | N=2 |
| Refactor | SQL compiles (sqlglot). Source table set preserved. | Row-count equivalence assertion | N=2 |
| Generate | dbt compile + dbt test pass. | Model refs match refactored SQL sources | N=3 |

2+ soft warnings accumulate to escalation even if no hard gate fails.

---

## Ingestion (formerly "Sourcing")

**Decision**: The migration utility outputs an **ingestion manifest** in two formats: agent-ready (YAML) and human-readable (markdown). It does not generate the actual data movement infrastructure. Sources may be databases, apps, files, or APIs — not assumed to be SQL Server.

**Two formats**:
- **`ingestion-manifest.yaml`** — Structured spec consumable by another agent or automation. Contains source groups, recommended methods, table lists, row estimates, refresh cadence.
- **`ingestion-manifest.md`** — Human-readable guide with rationale for method recommendations, setup checklists, and prerequisites.

**Questionnaire covers**:
- Per source group: origin type (database/app/file/API), access method
- Recommended sourcing method: Mirroring (database accessible from Fabric) / Shortcuts (data already in Azure) / Data Factory (app/API/file/scheduled)
- Refresh cadence and source ETL completion time

---

## Parallel Run (formerly "Comparison / Validation")

**Decision**: Fabric Semantic Model + Report for ongoing comparison over 3-5 days, not dbt-audit-helper. The migration utility outputs a **parallel run manifest** in two formats plus an agent-ready setup prompt.

**Rationale**: dbt-audit-helper can't query across SQL Server and Fabric in the same query. A Fabric Semantic Model can DirectQuery into both simultaneously. Building the comparison on Fabric is itself proof that vibeData works.

**Three outputs**:
- **`parallel-run-manifest.yaml`** — Structured spec: table pairs, primary keys, measures, SCD handling, date columns, expected daily deltas. Consumable by another agent.
- **`parallel-run-manifest.md`** — Human-readable guide: daily comparison protocol, semantic model setup instructions, report specification, escalation criteria.
- **`parallel-run-setup-prompt.md`** — Ready-to-use prompt for another agent or human to build the Fabric Semantic Model + Report. References the YAML manifest.

**Automated daily checks dropped**: No clean cross-database runtime at showcase scale. The human reviews the comparison report daily.

---

## Target Information

**Decision**: The migration utility needs naming conventions only, not actual infrastructure details (no workspace names, connection strings, lakehouse endpoints).

**What it needs**: Schema/layer names (bronze, staging, marts), table prefix conventions (stg_, dim_, fct_), case convention (snake_case).

**Rationale**: Actual deployment targets are configuration in `profiles.yml`, not the migration utility's concern. Naming conventions affect dbt model generation (source/ref names, schema config).

---

## Implementation Strategy

**Decision**: Incremental refactor. Build `/showcase` as a new orchestration command that calls existing CLI tools. Do not rebuild.

**Rationale**: The catalog schema and CLI tools are solid — only the prompt-level orchestration needs to change. New command traces dependency tree backwards using existing `discover refs` CLI, then chains existing scope/profile/refactor/generate steps with self-checks. Old per-table commands remain functional for edge cases.
