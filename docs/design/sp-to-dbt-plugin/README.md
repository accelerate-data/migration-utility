# SP → dbt Migration Plugin

Deterministic Python skills that automate stored-procedure-to-dbt migration — discover, scope, profile, migrate, test-gen, and validate — using sqlglot AST analysis and LLM inference. No live database required for the deterministic steps. Claude orchestrates the skills and handles judgment calls.

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
    ├── bootstrap/                         ← plugin: init + setup + DDL extraction
    │   ├── commands/init-ad-migration.md
    │   └── skills/
    │       └── setup-ddl/                ← DDL extraction from live SQL Server
    ├── migration/                         ← plugin: analysis + migration pipeline
    │   ├── CLAUDE.md
    │   ├── .mcp.json
    │   ├── shared/                        ← Python package (uv-managed)
    │   │   └── shared/
    │   │       ├── ir.py, loader.py, name_resolver.py, dialect.py
    │   │       ├── discover.py            ← skill script
    │   │       └── catalog.py             ← catalog JSON file I/O
    │   ├── skills/
    │   │   ├── discover/                  ← SKILL.md + rules/
    │   │   ├── profile/                   ← not yet implemented
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
| `catalog.py` | Per-object catalog JSON file I/O: read/write `catalog/` files, DMF result processing, reference flipping |
| `catalog_dmf.py` | DMF result classification: group referenced entities by type, detect cross-database/cross-server refs |
| `catalog_enrich.py` | Offline AST enrichment: fill DMF gaps (SELECT INTO, TRUNCATE, EXEC chains) via sqlglot + BFS |
| `name_resolver.py` | Normalize FQN: strip brackets, lowercase, apply default schema |
| `dialect.py` | `SqlDialect` protocol + registry keyed by string name |
| `export_ddl.py` | DDL + catalog extraction from live SQL Server via pyodbc (`--catalog` flag) |
| `profile.py` | Assemble profiling context from catalog files + DDL (no live DB, no LLM) |

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
Output (show):  { "name": "...", "type": "procedure", "raw_ddl": "...",
                  "refs": { "writes_to": [...], "reads_from": [...],
                            "write_operations": {"target": ["INSERT"]} },
                  "needs_llm": false, "classification": "deterministic",
                  "parse_error": null }
Output (refs):  { "name": "dbo.Foo", "source": "catalog",
                  "readers": [...], "writers": [{"procedure": "...",
                  "write_type": "direct", "is_updated": true}] }
```

### scope (via scoping agent)

Writer discovery is handled by the scoping agent (`scoping-agent.md`), not a standalone `scope.py` CLI. The agent calls `discover refs` (catalog-first, AST fallback) and `discover show` (for LLM-required procs), then applies resolution rules to produce a `CandidateWriters` JSON output.

When catalog files exist (from `setup-ddl`), writers are binary facts from `sys.dm_sql_referenced_entities` — no confidence scoring, no BFS. When catalog is absent, the AST fallback path uses confidence scoring:

| Signal | Effect |
|---|---|
| Direct write (INSERT/UPDATE/DELETE/MERGE/TRUNCATE/SELECT_INTO) | base 0.90 |
| Indirect write (callee is a confirmed direct writer) | base 0.75 |
| Shorter call path (per hop) | +0.02 |
| Multiple independent write paths | +0.05 |

Resolution: `resolved` (1 writer), `ambiguous_multi_writer` (2+), `no_writer_found` (0), `error`.

**Known limitation:** Procs that write only via dynamic SQL (`EXEC(@sql)`, `sp_executesql`) will not appear in catalog `referenced_by`. These require LLM analysis.

### profile

Two subcommands: `context` (assemble LLM input) and `write` (merge results into catalog file).

#### `profile context`

```text
Input:  context --ddl-path PATH  --table dbo.FactSales
        --writer dbo.usp_Load  --dialect tsql

Output: {
  "table": "dbo.FactSales",
  "writer": "dbo.usp_Load",
  "catalog": {
    "primary_keys": [...],
    "foreign_keys": [...],
    "auto_increment_columns": [...],
    "change_capture": null,
    "sensitivity_classifications": [],
    "referenced_by": { "procedures": [...], "views": [...] }
  },
  "writer_references": {
    "tables": [
      { "schema": "dbo", "name": "FactSales", "is_updated": true,
        "columns": [{ "name": "sale_id", "is_selected": true, "is_updated": false }] }
    ]
  },
  "proc_body": "CREATE PROCEDURE ...",
  "columns": [{ "name": "sale_id", "sql_type": "BIGINT" }],
  "related_procedures": []
}
```

Reads pre-captured catalog files + DDL, cherry-picks relevant signals, outputs a single JSON payload. No LLM calls, no file writes, no live database access.

#### `profile write`

```text
Input:  write --ddl-path PATH  --table dbo.FactSales
        --profile '{"status":"ok","writer":"dbo.usp_Load",...}'

Output: { "written": "catalog/tables/dbo.FactSales.json", "status": "ok" }
```

Reads existing catalog file, validates the profile JSON (required fields, allowed enum values), merges the `profile` section, writes back atomically. Exit codes: `0` = success, `1` = validation failure, `2` = IO error.

#### Shared between both paths

Both the interactive skill and the batch agent share `profile.py` subcommands. Each has its own LLM reasoning instructions between `context` and `write`:

- **Interactive (`/profile` skill):** `context` → Claude reasons over context + [What to Profile and Why](../agent-contract/what-to-profile-and-why.md) → present for approval → `write`.
- **Batch (profiler agent):** `context` per table → agent reasons with batch-tuned prompting (no approval gates, skip-and-continue) → `write` per table → aggregate summary. See [Profiler Agent Contract](../agent-contract/profiler-agent.md).

The LLM reasoning is the same six questions in both paths. The difference is orchestration: interactive stops for approval, batch continues and reports at the end.

### Statement classification (replaces assess)

`discover show` for procedures now returns a `statements` array that classifies each body statement:

| Action | Statement types | Meaning |
|---|---|---|
| `migrate` | INSERT, UPDATE, DELETE, MERGE, SELECT INTO | Core transformation → becomes the dbt model |
| `skip` | SET, TRUNCATE, DROP INDEX, CREATE INDEX/PARTITION, DECLARE | Operational overhead → handled by dbt materialization or ignored |
| `claude` | EXEC, sp_executesql, dynamic SQL | Needs Claude to follow call graph or resolve runtime SQL |

This replaces the separate `assess` skill. The `classification` field (`deterministic` or `claude_assisted`) gives the top-level verdict; the `statements` array gives the per-statement breakdown.

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
1. discover      → list tables → user picks one
2. scope         → find writers → user confirms which procedure to migrate
3. discover show → statement breakdown → user reviews migrate/skip/claude
4. profile       → catalog signals + LLM inference → user approves candidates
5. migrate       → generate dbt SQL (using profile answers) → user approves before file write
6. test-gen      → generate schema.yml → user approves before file write
7. validate      → compare outputs (skipped if no live DB)
```

**Gate rules:**

| After step | Gate |
|---|---|
| scope | Always — show writer list, user picks procedure |
| discover show | If `claude_assisted` — require explicit approval before proceeding |
| profile | Always — show classification, keys, watermark, PII candidates; user approves/edits |
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
| VU-733 | discover.py | Done — list/show/refs with catalog-first + AST fallback |
| VU-734 | discover SKILL.md | Done |
| VU-735 | discover tests | Done |
| VU-736 | scope.py | Cancelled — scoping handled by scoping agent + discover refs |
| VU-737 | scope SKILL.md | Cancelled |
| VU-738 | scope tests | Cancelled |
| VU-766 | catalog extraction | Done — catalog.py, setup-ddl + export_ddl extensions |
| VU-767 | discover refactor | Done — catalog-first refs/show |
| VU-768 | scoping contract update | Done — simplified for catalog-based writers |

---

### Wave 3 — Profile + Migrate

| Issue | What | Depends on |
|---|---|---|
| TBD | profile.py | VU-732 (shared lib), VU-733 (discover) |
| TBD | profile SKILL.md | profile.py |
| TBD | profile tests | profile.py |
| VU-742 | migrate.py | VU-732, profile.py |
| VU-743 | migrate SKILL.md | VU-742 |
| VU-744 | migrate tests | VU-742 |

**Exit criteria:** `profile.py` returns catalog signals + proc body + columns as structured JSON. `migrate.py` consumes profile answers and produces a valid `.sql` dbt model. Snapshot tests pass for all fixture procedures.

---

### Wave 4 — Test-Gen + Validate (parallel)

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

### Wave 5 — Orchestrator + GHA

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
  │     │
  │     └── VU-766 (catalog.py) ✅ ── VU-767 (catalog-first refs) ✅ ── VU-768 (scoping contract) ✅
  │
  ├── VU-736 (scope.py) ✗ cancelled — scoping agent + discover refs handles this
  │
  ├── TBD (profile.py) ──────── TBD (SKILL) ──────── TBD (tests)
  │
  ├── VU-742 (migrate.py) ──── VU-743 (SKILL) ───── VU-744 (tests)
  │     │
  │     ├── VU-745 (test_gen.py) ── VU-746 (SKILL) ── VU-747 (tests)
  │     │
  │     └── VU-748 (validate.py) ── VU-749 (SKILL) ── VU-750 (tests)
  │
VU-751 (ddl_mcp) ✅

assess cancelled — statement classification built into discover show.
scope.py cancelled — scoping handled by scoping-agent.md calling discover refs + show.
profile.py collects catalog signals; LLM inference is done by the agent (batch) or Claude (interactive).
All skills → VU-752 (migrate-table SKILL.md) → VU-753 (GHA workflow)
```

---

## Key Decisions

**sqlglot, not regex:** All parsing and analysis uses sqlglot AST. Unparseable procedures fail hard with `PARSE_FAILED` — no regex fallback.

**Deterministic tools, not LLM:** Scoping, assessment, and SQL transpilation have deterministic correct answers. Claude's role is judgment — reviewing output, handling transpile gaps, approving tests.

**Standalone scripts:** Each skill is independently testable and runnable from CLI. The orchestrator is a SKILL.md (plain text) and can be updated without touching Python.

**Validate is optional:** `validate.py` exits cleanly with `status: skipped` when `mssql_mcp` is absent.

**`ddl_mcp` shares `shared/`:** The MCP server and the skills both use `shared/loader.py` and `shared/name_resolver.py`. Same DDL files, no duplication.
