# SP → dbt Migration Plugin

Design for the `migrate-table` Claude Code plugin: a set of Python skills that automate the full stored-procedure-to-dbt workflow — discover, scope, assess, migrate, test-gen, and validate — all working from local DDL files with no live database required.

---

## Problem

The existing agent pipeline (scoping-agent → profiler-agent → … → migrator-agent) is LLM-driven end-to-end. For the migration steps that are deterministic — finding which procedures write to a table, checking whether a procedure can be converted, transpiling SQL — an LLM adds latency, cost, and non-determinism with no quality benefit.

This plugin provides deterministic Python tools (using sqlglot) for those steps. Claude orchestrates them and handles the judgment calls: reviewing transpile output, approving generated tests, and fixing gaps the tools cannot handle.

---

## Scope

**In scope:** T-SQL stored procedures → dbt Spark SQL models (Fabric Lakehouse target). Snowflake source dialect is a planned extension, not MVP.

**Out of scope:** bronze ingestion, ADF pipelines, Power BI, Fabric Lakehouse notebooks.

---

## Where It Lives

```text
agent-sources/plugins/ad-migration/
├── shared/                  ← shared Python library (new)
├── skills/
│   ├── discover/            ← new skill
│   ├── scope/               ← new skill
│   ├── assess/              ← new skill
│   ├── migrate/             ← new skill
│   ├── test-gen/            ← new skill
│   └── validate/            ← new skill
├── commands/
│   └── migrate-table/       ← new command (orchestrator)
└── ddl_mcp/
    └── server.py            ← enhanced in-place
```

Each skill is a `SKILL.md` + a standalone Python script. The script outputs JSON to stdout. Claude runs the script via Bash, reads the JSON, and decides next steps.

---

## Shared Library (`shared/`)

All skills import from `shared/`. Nothing in `shared/` is skill-specific.

| Module | Responsibility |
|---|---|
| `shared/ir.py` | Pydantic IR types: `Procedure`, `ProcParam`, `SelectModel`, `CteNode`, `TableRef`, `ColumnRef` |
| `shared/loader.py` | Parse a DDL directory → `DdlCatalog` (GO-split + `sqlglot.parse_one`) |
| `shared/name_resolver.py` | Normalize FQN: strip brackets, lowercase, apply default schema |
| `shared/dialect.py` | `SqlDialect` protocol + registry keyed by string name |

**Why Pydantic:** built-in JSON serialization, validation, and schema generation. All inter-skill data flows as JSON — tools do not share Python objects.

**Why sqlglot:** best T-SQL coverage of any Python library; built-in transpiler for T-SQL → Spark SQL; same library serves parsing, analysis, and generation.

---

## Skill Contracts

Each skill script is invoked as:

```bash
python <skill>.py [flags] 2>/dev/null
```

- **stdout:** always valid JSON (one object, not JSONL)
- **stderr:** human-readable progress/warnings (not parsed by orchestrator)
- **exit codes:** `0` = success, `1` = domain failure (unsupported, no writers), `2` = parse/IO error

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
    { "procedure": "dbo.usp_Cross", "code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE", "message": "..." }
  ]
}
```

Confidence scoring rules (from `scoring.md`, implemented in code — not prompt):

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

Rules (deterministic AST visitor — not LLM):

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

Generation steps (in order):

1. Parse procedure with `sqlglot.parse_one(sql, dialect="tsql")`
2. Replace parameter references with `{{ var('name', default) }}` nodes
3. `sqlglot.transpile(sql, read="tsql", write="spark")`
4. Post-process: apply `--refs` map → `{{ ref() }}` / `{{ source() }}`
5. Determine materialization hint from AST (date filter → incremental, GROUP BY only → table, else view)
6. Wrap: leading comment block → `{{ config(materialized=...) }}` → CTEs → final SELECT
7. Write `.sql` to output directory

Runs only if `assess` status is not `Unsupported`. If called on an unsupported procedure, exits with code 1 and no file written.

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

The orchestrator. Defined entirely in `commands/migrate-table/SKILL.md`. No Python implementation — Claude follows the instructions.

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

## Enhanced `ddl_mcp`

In-place upgrade of `ddl_mcp/server.py`. All existing tool names and signatures preserved.

| Tool | Change |
|---|---|
| `get_dependencies` | Text grep → sqlglot AST walk (eliminates false positives from string literals and comments) |
| `get_table_schema` | Returns structured JSON with column list in addition to raw DDL text |
| `list_functions` | New tool — lists all functions from `functions.sql` |
| `get_function_body` | New tool — returns DDL for a named function |

Reuses `shared/loader.py` and `shared/name_resolver.py`. Existing callers that only use the raw DDL string field are unaffected — new fields are additive.

---

## Relation to Existing Agent Pipeline

The new skills and the existing LLM agents are **complementary, not competing**.

| Stage | Existing agent | New skill |
|---|---|---|
| Discover | `ddl_mcp` (text grep) | `discover.py` (semantic AST) |
| Scope | `scoping-agent` (LLM) | `scope.py` (deterministic) |
| Assess | none | `assess.py` (deterministic) |
| Migrate | `migrator-agent` (LLM) | `migrate.py` (sqlglot transpile) + Claude review |
| Test gen | `test-generator-agent` (LLM + live DB) | `test_gen.py` (AST inference, no live DB) |
| Validate | none | `validate.py` (result set diff) |

The new skills are the fast, cheap, deterministic first path. The LLM agents are the fallback for edge cases the tools cannot handle.

---

## Implementation Plan

### Wave 1 — Foundation (blocks everything else)

| Issue | What | Why first |
|---|---|---|
| VU-732 | Shared library | All skills import from here; nothing else can be built without it |
| VU-751 | Enhanced ddl_mcp | Early upgrade improves the existing scoping-agent immediately and unblocks discover |

**Exit criteria:** `from shared import ir, loader, dialect` works. `ddl_mcp` tests pass.

---

### Wave 2 — Discover + Scope (parallel)

These two skills are independent of each other and can be built simultaneously.

| Issue | What | Depends on |
|---|---|---|
| VU-733 | discover.py | VU-732 |
| VU-734 | discover SKILL.md | VU-733 |
| VU-735 | discover tests | VU-733 |
| VU-736 | scope.py | VU-732 |
| VU-737 | scope SKILL.md | VU-736 |
| VU-738 | scope tests | VU-736 |

**Exit criteria:** `python discover.py list --type tables --ddl-path artifacts/ddl/` returns JSON. `python scope.py --table dbo.FactSales --ddl-path artifacts/ddl/` returns writers with correct confidence scores against a known fixture.

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

**Exit criteria:** `python migrate.py --proc proc.sql --output out/` produces a valid `.sql` dbt model. Snapshot tests pass for all fixture procedures.

---

### Wave 5 — Test-Gen + Validate (parallel)

These two skills are independent of each other.

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

**Exit criteria:** running `migrate-table` interactively against a real DDL directory completes the full 6-step flow. GHA workflow triggers successfully and commits a dbt model.

---

## Dependency Graph

```text
VU-732 (shared lib)
  ├── VU-733 (discover.py) ─── VU-734 (SKILL) ─── VU-735 (tests)
  │
  ├── VU-736 (scope.py) ────── VU-737 (SKILL) ─── VU-738 (tests)
  │
  ├── VU-739 (assess.py) ───── VU-740 (SKILL) ─── VU-741 (tests)
  │     │
  │     └── VU-742 (migrate.py) ─── VU-743 (SKILL) ─── VU-744 (tests)
  │           │
  │           ├── VU-745 (test_gen.py) ─── VU-746 (SKILL) ─── VU-747 (tests)
  │           │
  │           └── VU-748 (validate.py) ─── VU-749 (SKILL) ─── VU-750 (tests)
  │
VU-751 (enhanced ddl_mcp) [parallel to shared lib]

All skills → VU-752 (migrate-table SKILL.md) → VU-753 (GHA workflow)
```

---

## Key Decisions

**sqlglot over sqlparser-rs:** The engine is decoupled from Tauri. Python runs in GHA natively, the existing `ddl_mcp` is Python, and sqlglot's T-SQL transpiler is more complete than any Rust equivalent.

**Deterministic tools, not LLM prompts:** Scoping, assessment, and SQL transpilation have deterministic correct answers. Using an LLM for these adds cost and variance. Claude's role is judgment — reviewing output, handling transpile gaps, approving tests — not computation.

**Each skill is a standalone script:** Scripts are independently testable and runnable from the command line without touching the orchestrator. The orchestrator is a SKILL.md (plain text) and can be updated without touching Python.

**Validate is optional:** Not every migration has a live DB available. `validate.py` exits cleanly with `status: skipped` when `mssql_mcp` is absent. The skill adds confidence when connectivity exists; it does not block migration when it does not.

**`ddl_mcp` upgraded in-place:** The MCP serves LLM agents via the MCP protocol. The skills use `shared/loader.py` directly. Both consume the same DDL files. No duplication.
