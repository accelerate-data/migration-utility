# Migration Utility — Build Plan

*1 engineer with Claude Code. 5 weeks to MVP.*

---

## Architecture

| Component | What | Tech |
|-----------|------|------|
| Desktop app | Project management, FDE review, stage submission | Tauri v2 + React + SQLite |
| Agents | Scope, Profile, Decompose, Plan, Generate Tests, Migrate | Claude Agent SDK, one GH Actions workflow per agent |
| Source inspection | DacPac restore + T-SQL metadata queries | Docker SQL Server container |
| Runtime | Headless agent execution via `workflow_dispatch` | GitHub Actions |
| State | Local app state + stage artifacts | SQLite (local) + JSON in migration repo (git-backed) |
| Source assets | DacPac storage and caching | Git LFS |

---

## Pre-Reqs

1. Docker Desktop — local SQL Server container for DacPac inspection
2. GitHub account — OAuth with `repo` + `workflow` scopes
3. Anthropic API key — stored as GitHub Secret
4. DacPac export of the source SQL Server database
5. One real customer domain with 10+ stored procedures (SQL-heavy majority)
6. Vibedata production repo template
7. GitHub Actions runner with `ANTHROPIC_API_KEY` secret

---

## Timeline

| Phase | What | Weeks |
|-------|------|-------|
| 0 | Foundation — validate stack, scaffold Tauri app, DacPac inspection pipeline, define artifact JSON schemas | 0.5 |
| 1 | MVP pipeline — Scope → Profile → Decompose → Plan → Generate Tests → Migrate (SQL-heavy only) | 3 |
| 2 | Real customer domain end-to-end + Tauri setup UI + hardening | 1.5 |
| **MVP** | **Full pipeline, SQL-heavy patterns** | **5** |
| 3 | Pattern expansion | ongoing |

### Phase 1 Breakdown

| Week | Deliverable |
|------|-------------|
| 1 | Scope + Profile agents — DacPac inspection, stored proc discovery, dependency graph, profile output with per-item evidence |
| 2 | Decompose + Plan agents + FDE review loop — editable plan + per-item approvals |
| 3 | Generate Tests + Migrate agents + validation + full gated pipeline end-to-end on mocks |

### Phase 2 Breakdown

| Week | Deliverable |
|------|-------------|
| 4 | E-01, E-02 on real stored procs. Tauri app polish (project management + stage review UX) |
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
| E-01 | 5 stored procs, all Migrate, linear dependency chain |
| E-02 | 10 stored procs, mixed tiers, diamond dependency graph |
| E-03 | Session interrupted at 40%, resumed |

---

## Key Risks

| Risk | Mitigation |
|------|------------|
| DacPac inspection gaps (missing metadata, unsupported object types) | Validate Day 1 with real DacPac; fall back to manual SQL extraction |
| Docker Desktop availability on FDE machines | Document prerequisites; test on macOS + Windows |
| Translation quality | Eval loop Week 2 with real stored procs |
| GitHub Actions cost / concurrency limits | Self-hosted runner if needed; batch items per run |
| Real stored procs diverge from mocks | Real stored procs by Week 4 |
