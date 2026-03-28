# SP → dbt Migration Plugin

Deterministic Python skills that automate stored-procedure-to-dbt migration — discover, scope, assess, migrate, test-gen, and validate — using sqlglot AST analysis. No live database required. Claude orchestrates the skills and handles judgment calls.

---

## Scope

**In scope:** T-SQL stored procedures → dbt Spark SQL models (Fabric Lakehouse target).

**Out of scope:** bronze ingestion, ADF pipelines, Power BI, Fabric Lakehouse notebooks, Snowflake (planned extension).

---

## Where It Lives

`ad-migration` is a Claude Code marketplace package. `workbench/` contains three plugins registered in `marketplace.json`.

```text
agent-sources/ad-migration/               ← marketplace package
├── .claude-plugin/marketplace.json
├── CLAUDE.md                              ← shared domain context
└── workbench/
    ├── bootstrap/                         ← plugin: init + setup
    │   └── commands/init-ad-migration.md
    ├── migration/                         ← plugin: analysis + migration pipeline
    │   ├── CLAUDE.md
    │   ├── .mcp.json
    │   ├── shared/                        ← Python package (uv-managed)
    │   │   └── shared/
    │   │       ├── ir.py, loader.py, name_resolver.py, dialect.py
    │   │       ├── discover.py            ← skill script
    │   │       └── scope.py               ← skill script
    │   ├── skills/
    │   │   ├── discover/                  ← SKILL.md + rules/
    │   │   ├── scope/                     ← SKILL.md + rules/
    │   │   ├── scoping-writers/           ← reference docs for scoping algorithm
    │   │   ├── setup-ddl/                 ← DDL extraction from live SQL Server
    │   │   ├── assess/                    ← not yet implemented
    │   │   ├── migrate/                   ← not yet implemented
    │   │   ├── test-gen/                  ← not yet implemented
    │   │   └── validate/                  ← not yet implemented
    │   ├── commands/
    │   │   └── migrate-table/             ← orchestrator (not yet implemented)
    │   ├── ddl_mcp/
    │   │   └── server.py                  ← DDL file MCP server
    │   └── mssql_mcp/
    │       └── tools.yaml                 ← live SQL Server MCP config
    └── test-generation/                   ← plugin: dbt test generation (placeholder)
        └── CLAUDE.md
```

Tests live at `tests/ad-migration/migration/`.

Each skill has a `SKILL.md` (Claude instructions) + a Python script in `shared/shared/`. The script outputs JSON to stdout. Claude runs it via `uv run`, reads the JSON, and decides next steps.

---

## Shared Library

All skills import from `shared/`. Nothing in `shared/` is skill-specific.

| Module | Responsibility |
|---|---|
| `ir.py` | Pydantic IR types: `Procedure`, `ProcParam`, `SelectModel`, `CteNode`, `TableRef`, `ColumnRef` |
| `loader.py` | Parse a DDL directory → `DdlCatalog` (GO-split + `sqlglot.parse_one`) |
| `name_resolver.py` | Normalize FQN: strip brackets, lowercase, apply default schema |
| `dialect.py` | `SqlDialect` protocol + registry keyed by string name |

---

## Skill Contracts

Each skill script is invoked as:

```bash
uv run --project shared <skill> [flags] 2>/dev/null
```

- **stdout:** valid JSON (one object, not JSONL)
- **stderr:** human-readable progress/warnings (not parsed by orchestrator)
- **exit codes:** `0` = success, `1` = domain failure, `2` = parse/IO error

### discover

```text
Input:  --ddl-path PATH  --dialect tsql
        subcommand: list|show|refs
        --type tables|procedures|views|functions  (list only)
        --name dbo.MyTable                         (show|refs only)

Output (list):  { "objects": ["dbo.Foo", "dbo.Bar"] }
Output (show):  { "name": "dbo.Foo", "raw_ddl": "...", "columns": [...] }
Output (refs):  { "name": "dbo.Foo", "referenced_by": ["dbo.usp_Load", ...] }
```

### scope

```text
Input:  --ddl-path PATH  --table dbo.FactSales  --dialect tsql  --depth 3

Output: {
  "table": "dbo.FactSales",
  "writers": [
    {
      "procedure": "dbo.usp_Load",
      "write_type": "direct|indirect",
      "write_operations": ["INSERT", "MERGE"],
      "call_path": [],
      "confidence": 0.90,
      "status": "confirmed|suspected"
    }
  ],
  "errors": [
    { "procedure": "dbo.usp_Cross", "code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE", "message": "..." },
    { "procedure": "dbo.usp_Complex", "code": "PARSE_FAILED", "message": "..." }
  ]
}
```

All write detection and call-graph resolution use sqlglot AST analysis. Procedures that cannot be parsed are reported as `PARSE_FAILED` — no regex fallback.

This is an intermediate format. Resolution logic (resolved / ambiguous / no_writer / etc.) lives in the `migrate-table` orchestrator, not in `scope.py`.

Confidence scoring rules (from `scoring.md`, implemented in code):

| Signal | Effect |
|---|---|
| Direct write (INSERT/UPDATE/DELETE/MERGE/TRUNCATE) | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop) | +0.02 |
| Multiple independent write paths | +0.05 |
| Dynamic SQL alongside static evidence | −0.20 |
| Dynamic SQL only | cap at 0.45 |

Status: `confirmed` if confidence ≥ 0.70, else `suspected`.

### assess

```text
Input:  --proc PATH  --dialect tsql

Output: {
  "procedure": "dbo.usp_Load",
  "status": "Supported|Partial|Unsupported",
  "findings": [
    { "severity": "BLOCKED|WARNING", "code": "DYNAMIC_SQL", "message": "...", "line": 42 }
  ]
}
```

Rules (deterministic AST visitor):

| Category | Constructs |
|---|---|
| Clean (no finding) | SELECT, all JOINs, CTEs, WHERE, GROUP BY, HAVING, CASE, window functions, scalar subqueries, CAST, COALESCE |
| WARNING | Read-only `#temp` refs, `@table_variable`, STRING_AGG/STUFF/FOR XML PATH, TOP N, NOLOCK hints |
| BLOCKED | EXEC/sp_executesql, cursors, WHILE/IF+DML, body INSERT/UPDATE/DELETE, RAISERROR/THROW, TRY/CATCH, OPENROWSET, four-part linked-server names |

`status` = worst finding severity: any BLOCKED → Unsupported, any WARNING → Partial, else Supported.

### migrate

```text
Input:  --proc PATH  --output DIR
        --dialect tsql  --target spark
        --refs '{"dbo.dim_customer": "dim_customer"}'

Output: {
  "model_path": "out/usp_Load.sql",
  "param_mappings": { "@start_date": "var('start_date', '2020-01-01')" },
  "transpile_warnings": [...],
  "materialization_hint": "incremental|table|view"
}
```

Generation steps:

1. Parse procedure with `sqlglot.parse_one(sql, dialect="tsql")`
2. Replace parameter references with `{{ var('name', default) }}` nodes
3. `sqlglot.transpile(sql, read="tsql", write="spark")`
4. Post-process: apply `--refs` map → `{{ ref() }}` / `{{ source() }}`
5. Determine materialization hint from AST (date filter → incremental, GROUP BY only → table, else view)
6. Wrap: leading comment block → `{{ config(materialized=...) }}` → CTEs → final SELECT
7. Write `.sql` to output directory

Exits with code 1 if `assess` status is `Unsupported`.

### test-gen

```text
Input:  --proc PATH  --model PATH  --output DIR

Output: {
  "schema_yml_path": "out/schema.yml",
  "tests": [
    { "column": "customer_id", "test": "not_null", "reason": "used in JOIN predicate" }
  ]
}
```

Test inference rules (AST-based):

| Pattern | Test generated |
|---|---|
| Column in JOIN predicate | `not_null` |
| Column in GROUP BY | `unique` |
| Column in CASE WHEN with ≤10 distinct branches | `accepted_values` |
| FK reference visible in JOIN | `relationships` |

### validate

```text
Input:  --proc dbo.usp_Load  --model PATH
        --params '{"start_date": "2024-01-01"}'

Output: {
  "status": "pass|fail|warning|skipped",
  "row_diff_count": 0,
  "column_mismatches": [],
  "sample_diffs": []
}
```

Execution:

1. Resolve `{{ var('name', default) }}` in model SQL using `--params`
2. Run original proc via `mssql_mcp` `execute_query`
3. Run resolved model SQL via `mssql_mcp` `execute_query`
4. Compare result sets (unordered, float tolerance applied)

If `mssql_mcp` is not configured: returns `{ "status": "skipped" }`, exit 0.

---

## `migrate-table` Command

The orchestrator. Defined in `commands/migrate-table/SKILL.md`. No Python — Claude follows the instructions.

**Interactive flow:**

```text
1. discover  → list tables → user picks one
2. scope     → find writers → user confirms which procedure to migrate
3. assess    → classify procedure → user approves if Partial/Unsupported
4. migrate   → generate dbt SQL → user approves before file write
5. test-gen  → generate schema.yml → user approves before file write
6. validate  → compare outputs (skipped if no live DB)
```

**Gate rules:**

| After step | Gate |
|---|---|
| scope | Always — show writer list, user picks procedure |
| assess | If Partial/Unsupported — require explicit approval |
| migrate | Always — show generated model before writing to disk |
| test-gen | Always — show schema.yml before writing to disk |
| validate | None — result shown, no gate |

**`--non-interactive` mode (GHA):** skips all gates, stops at first `Unsupported` procedure with exit 1.

---

## Implementation Plan

### Wave 1 — Foundation ✅

| Issue | What | Status |
|---|---|---|
| VU-732 | Shared library | Done — `ir`, `loader`, `name_resolver`, `dialect` all functional with tests |
| VU-751 | Enhanced ddl_mcp | Done — AST-based `get_dependencies`, structured column JSON, function tools |

---

### Wave 2 — Discover + Scope (parallel)

| Issue | What | Status |
|---|---|---|
| VU-733 | discover.py | Done — functional with CLI (list/show/refs) |
| VU-734 | discover SKILL.md | Done |
| VU-735 | discover tests | Done |
| VU-736 | scope.py | Needs AST-only rewrite (currently uses regex fallback) |
| VU-737 | scope SKILL.md | Done |
| VU-738 | scope tests | Done — 8 fixture scenarios |

**Remaining:** rewrite `scope.py` write detection and call-graph resolution to use sqlglot AST only. Remove all regex patterns.

---

### Wave 3 — Assess

| Issue | What | Depends on |
|---|---|---|
| VU-739 | assess.py | VU-732 |
| VU-740 | assess SKILL.md | VU-739 |
| VU-741 | assess tests | VU-739 |

**Exit criteria:** assess correctly classifies a clean procedure as Supported, a procedure with dynamic SQL as Unsupported, and a procedure with NOLOCK as Partial.

---

### Wave 4 — Migrate

| Issue | What | Depends on |
|---|---|---|
| VU-742 | migrate.py | VU-732, VU-739 |
| VU-743 | migrate SKILL.md | VU-742 |
| VU-744 | migrate tests | VU-742 |

**Exit criteria:** `migrate.py` produces a valid `.sql` dbt model. Snapshot tests pass for all fixture procedures.

---

### Wave 5 — Test-Gen + Validate (parallel)

| Issue | What | Depends on |
|---|---|---|
| VU-745 | test_gen.py | VU-732, VU-742 |
| VU-746 | test-gen SKILL.md | VU-745 |
| VU-747 | test-gen tests | VU-745 |
| VU-748 | validate.py | VU-742 |
| VU-749 | validate SKILL.md | VU-748 |
| VU-750 | validate tests | VU-748 |

**Exit criteria:** `test_gen.py` emits correct `schema.yml` for fixtures. `validate.py` comparison logic passes all unit tests without a live DB.

---

### Wave 6 — Orchestrator + GHA

| Issue | What | Depends on |
|---|---|---|
| VU-752 | migrate-table SKILL.md | All skills complete |
| VU-753 | GHA workflow | VU-752 |

**Exit criteria:** `migrate-table` interactively completes the full 6-step flow against a real DDL directory. GHA workflow triggers and commits a dbt model.

---

## Dependency Graph

```text
VU-732 (shared lib) ✅
  ├── VU-733 (discover.py) ✅ ── VU-734 (SKILL) ✅ ── VU-735 (tests) ✅
  │
  ├── VU-736 (scope.py) ─────── VU-737 (SKILL) ✅ ── VU-738 (tests) ✅
  │
  ├── VU-739 (assess.py) ────── VU-740 (SKILL) ───── VU-741 (tests)
  │     │
  │     └── VU-742 (migrate.py) ── VU-743 (SKILL) ── VU-744 (tests)
  │           │
  │           ├── VU-745 (test_gen.py) ── VU-746 (SKILL) ── VU-747 (tests)
  │           │
  │           └── VU-748 (validate.py) ── VU-749 (SKILL) ── VU-750 (tests)
  │
VU-751 (ddl_mcp) ✅

All skills → VU-752 (migrate-table SKILL.md) → VU-753 (GHA workflow)
```

---

## Key Decisions

**sqlglot, not regex:** All parsing and analysis uses sqlglot AST. Unparseable procedures fail hard with `PARSE_FAILED` — no regex fallback.

**Deterministic tools, not LLM:** Scoping, assessment, and SQL transpilation have deterministic correct answers. Claude's role is judgment — reviewing output, handling transpile gaps, approving tests.

**Standalone scripts:** Each skill is independently testable and runnable from CLI. The orchestrator is a SKILL.md (plain text) and can be updated without touching Python.

**Validate is optional:** `validate.py` exits cleanly with `status: skipped` when `mssql_mcp` is absent.

**`ddl_mcp` shares `shared/`:** The MCP server and the skills both use `shared/loader.py` and `shared/name_resolver.py`. Same DDL files, no duplication.
