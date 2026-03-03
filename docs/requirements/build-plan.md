# Migration Utility — Build Plan

*1 engineer with Claude Code. 5 weeks to MVP.*

---

## Architecture

| Component | What | Tech |
|-----------|------|------|
| Setup UI | Scope selection + FDE approvals for scoping/profiling/planning + plan edit | Tauri + SQLite |
| Orchestrator | Runs stage-gated batches (scoping -> profiling -> planning -> migration/testing) | Claude Agent SDK (Python) |
| Sub-agents | Scoping, Profiler, Planner, Translation, Test Generator, Validation | Agent SDK `AgentDefinition` with scoped tools |
| dbt interaction | Lineage, compiled SQL, model execution, validation queries | dbt-core-mcp (MCP server) |
| Runtime | Headless execution, no UI timeout | GitHub Actions (minimal YAML) |
| State | Stage progress, approvals, per-item outcomes, resumption | Structured batch JSON artifacts in repo (git-backed) |

---

## Pre-Reqs

1. Claude Agent SDK (Python) + Anthropic API key
2. dbt-fabricspark adapter
3. dbt-core-mcp — confirmed compatible with dbt-fabricspark and dbt Core 1.9+
4. Fabric API service principal — workspace metadata, stored procedures, ADF pipelines, table schemas
5. One real customer domain with 10+ stored procedures (SQL-heavy majority)
6. Dedicated F2 PAYG workspace with pause/resume
7. Vibedata production repo template
8. GitHub Actions runner with Fabric API access and `ANTHROPIC_API_KEY` secret

---

## Timeline

| Phase | What | Weeks |
|-------|------|-------|
| 0 | Foundation — validate stack, scaffold orchestrator, create mock stored procs + ADF pipelines, define batch JSON schemas | 0.5 |
| 1 | MVP pipeline — scoping → profiling → planning → migration/testing → push (SQL-heavy only) | 3 |
| 2 | Real customer domain end-to-end + Tauri setup UI + hardening | 1.5 |
| **MVP** | **Full pipeline, SQL-heavy patterns** | **5** |
| 3 | Pattern expansion | ongoing |

### Phase 1 Breakdown

| Week | Deliverable |
|------|-------------|
| 1 | Scoping + profiler agents — ADF pipeline scan, stored proc discovery, dependency graph, profile output with per-item evidence |
| 2 | Planner agent + FDE approval loop — editable plan JSON + per-item approvals |
| 3 | Migration/testing stage + validation + push to prod branch -> full gated pipeline end-to-end on mocks |

### Phase 2 Breakdown

| Week | Deliverable |
|------|-------------|
| 4 | E-01, E-02 on real stored procs. Tauri setup UI (scope + scoping/profiling/planning approvals) |
| 5 | E-03 session resumption. Error handling. Edge cases from real data |

### Phase 3: Pattern Expansion (ongoing)

| Pattern | Effort |
|---------|--------|
| Dynamic SQL stored procs (SP-03) | 2–3 days |
| Cross-database references (SP-05) | 1–2 days |
| Cursor-based stored procs (SP-06) | 1–2 days |
| Lakehouse notebook support (N-01 through N-07) | 2–3 weeks |

---

## Mock Scenarios

### Stored Procedures (Warehouse MVP)

| ID | Scenario | Phase |
|----|----------|-------|
| SP-01 | Pure T-SQL — SELECT/JOIN/GROUP BY, INSERT INTO target | MVP |
| SP-02 | T-SQL with CTEs and temp tables (#temp) | MVP |
| SP-04 | Incremental MERGE (SCD Type 1/2) | MVP |
| SP-10 | Full-refresh (TRUNCATE + INSERT or DROP + CTAS) | MVP |
| SP-03 | Dynamic SQL (`EXEC`/`sp_executesql`) | Post-MVP |
| SP-05 | Cross-database references | Post-MVP |
| SP-06 | Cursor-based row iteration | Post-MVP |

### Notebooks (Lakehouse Post-MVP)

| ID | Scenario | Phase |
|----|----------|-------|
| N-01 | SparkSQL with temp views and CTEs | Post-MVP |
| N-02 | PySpark DataFrame API | Post-MVP |
| N-03 | Mixed 70% SQL + Python string formatting | Post-MVP |
| N-04 | `%run` notebook reference | Post-MVP |
| N-05 | UDF-heavy | Post-MVP |
| N-06 | ML pipeline | Post-MVP |
| N-07 | RDD operations | Post-MVP |

### ADF Pipelines

| ID | Scenario | Phase |
|----|----------|-------|
| P-01 | Linear: SP-A → SP-B → SP-C (sequential stored proc activities) | MVP |
| P-02 | Parallel fan-out: SP-A, SP-B, SP-C → SP-D (wait dependency) | MVP |
| P-03 | Conditional execution (If/Switch activity) | MVP |
| P-04 | Parameterized pipeline (runtime parameters passed to stored procs) | MVP |

### Dependency Graphs

| ID | Scenario |
|----|----------|
| G-01 | Linear: bronze → silver → gold |
| G-02 | Fan-out: 1 bronze → 3 silver → 1 gold |
| G-03 | Diamond: A → B, A → C, B+C → D |
| G-04 | Mixed tiers: Migrate depends on Review upstream |
| G-05 | Circular dependency (should error) |
| G-06 | External source (table not in any stored proc) |
| G-07 | 50+ stored procs, mixed tiers |
| G-08 | Resumed session — batch stage rerun with partial success and blocked items |

### Snapshot + Fixtures

| ID | Scenario |
|----|----------|
| S-01 | Dimension, 500 rows, no PII |
| S-02 | Dimension with PII columns |
| S-03 | Fact, 10M rows, 1-day sample |
| S-04 | Ambiguous incremental column |
| S-05 | Silver derived from fact sample |
| S-07 | Full-refresh table |
| S-08 | YAML fixture — dbt compatibility |

### Test Gate

| ID | Scenario |
|----|----------|
| T-01 | Passes |
| T-02 | Unit test fails — gate blocks |
| T-03 | `dbt build` fails — gate blocks |
| T-04 | Upstream source reference |

### End-to-End

| ID | Scenario |
|----|----------|
| E-01 | 5 stored procs, all Migrate, linear ADF pipeline |
| E-02 | 10 stored procs, mixed tiers, diamond ADF pipeline |
| E-03 | Session interrupted at 40%, resumed |

---

## Key Risks

| Risk | Mitigation |
|------|------------|
| dbt-fabricspark instability | Validate Day 1 |
| dbt-core-mcp incompatible with dbt-fabricspark or dbt <1.9 | Validate Day 1 — fallback: custom wrappers (+1 week) |
| Translation quality | Eval loop Week 2 with real stored procs |
| Fabric API gaps for stored proc discovery | Week 1 — fall back to ADF pipeline JSON export/parse |
| GitHub Actions cost | Self-hosted runner if needed |
| Real stored procs diverge from mocks | Real stored procs by Week 4 |
