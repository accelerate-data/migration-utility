# SP → dbt Migration Plugin

Deterministic Python skills that automate stored-procedure-to-dbt migration — discover, scope, profile, migrate, test-gen, and validate — using sqlglot AST analysis and LLM inference. No live database required for the deterministic steps. Claude orchestrates the skills and handles judgment calls.

---

## Scope

**In scope:** T-SQL stored procedures → dbt Spark SQL models (Fabric Lakehouse target).

**Out of scope:** bronze ingestion, ADF pipelines, Power BI, Fabric Lakehouse notebooks, Snowflake (planned extension).

---

## Where It Lives

The repo root is a Claude Code marketplace package. Three plugins are registered in `.claude-plugin/marketplace.json`.

```text
.claude-plugin/marketplace.json
├── bootstrap/                             ← plugin: init + setup + DDL extraction
│   ├── commands/                          ← init-ad-migration command
│   └── skills/
│       └── setup-ddl/                    ← DDL extraction from live SQL Server
├── migration/                             ← plugin: analysis + migration pipeline
│   ├── agents/                            ← scoping-agent.md
│   ├── skills/
│   │   ├── discover/                      ← SKILL.md + references/
│   │   ├── profile/
│   │   ├── migrate/
│   │   ├── test-gen/
│   │   └── validate/
│   └── commands/
│       └── migrate-table/                 ← orchestrator
├── mcp/
│   ├── ddl/                               ← DDL file MCP server
│   └── mssql/                             ← live SQL Server MCP config
├── lib/                                   ← Python package (uv-managed)
│   └── shared/                            ← Python modules (see Shared Library table)
│       └── schemas/                       ← JSON Schema files
└── test-generation/                       ← plugin: dbt test generation (placeholder)
```

Tests live at `tests/unit/`.

Each skill has a `SKILL.md` (Claude instructions) + a Python script in `lib/shared/`. The script outputs JSON to stdout. Claude runs it via `uv run`, reads the JSON, and decides next steps.

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
| `setup_ddl.py` | DDL setup orchestration: assemble modules, tables, manifest, and catalog from staging JSON |
| `dmf_processing.py` | DMF row processing helpers shared by export and catalog write paths |
| `sql_types.py` | SQL type mappings between T-SQL and target dialects |

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
Input:  --project-root PATH  --dialect tsql
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
Input:  context --project-root PATH  --table dbo.FactSales
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
Input:  write --project-root PATH  --table dbo.FactSales
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

Resolved statements are persisted to `catalog/procedures/<writer>.json` by the scoping agent (batch) or `/discover` skill (interactive). All `claude` actions are resolved to `migrate` or `skip` before persisting. Downstream stages (profiler, migrator) read statements from catalog.

### migrate

Two subcommands: `context` (assemble migration context) and `write` (persist generated artifacts).

#### `migrate context`

```text
Input:  context --project-root PATH  --table dbo.FactSales
        --writer dbo.usp_Load

Output: {
  "table": "dbo.FactSales",
  "writer": "dbo.usp_Load",
  "profile": {
    "classification": "fact_transaction",
    "primary_key": { "columns": ["sale_id"], "primary_key_type": "surrogate" },
    "watermark": { "column": "load_date" },
    "foreign_keys": [...],
    "pii_actions": [...]
  },
  "materialization": "incremental",
  "statements": [
    { "action": "migrate", "source": "ast", "sql": "INSERT INTO ..." }
  ],
  "proc_body": "CREATE PROCEDURE ...",
  "columns": [{ "name": "sale_id", "data_type": "BIGINT" }],
  "source_tables": ["dbo.DimCustomer", "dbo.DimDate"],
  "schema_tests": {
    "entity_integrity": [{ "column": "sale_id", "tests": ["unique", "not_null"] }],
    "referential_integrity": [{ "column": "customer_sk", "to": "ref('dim_customer')", "field": "customer_sk" }],
    "recency": { "column": "load_date" },
    "pii": [{ "column": "customer_email", "action": "mask" }]
  }
}
```

Reads profile from `catalog/tables/`, statements from `catalog/procedures/`, proc body and columns from DDL. Derives materialization deterministically from classification. Builds schema tests from profile answers. No LLM calls, no SQL transpilation.

#### `migrate write`

```text
Input:  write --project-root PATH  --table dbo.FactSales
        --dbt-project-path PATH
        --model-sql '<sql>'  --schema-yml '<yml>'

Output: { "written": ["models/staging/fct_fact_sales.sql", "models/staging/_fct_fact_sales.yml"], "status": "ok" }
```

Validates and writes `.sql` + `.yml` to the dbt project models directory. Exit codes: `0` = success, `1` = validation failure, `2` = IO error.

#### Shared between both paths

Both the interactive skill and the batch agent share `migrate.py` subcommands. Each has its own LLM generation logic between `context` and `write`:

- **Interactive (`/migrate` skill):** `context` → LLM generates dbt model (decides structure, applies CTE conventions, ref/source/var substitution) → logical equivalence check → present for approval → `write`.
- **Batch (migrator agent):** `context` per table → agent generates with batch-tuned prompting (no approval gates, skip-and-continue) → equivalence check → `write` per table → `dbt compile` → aggregate summary.

The LLM generation is the same logic in both paths. No sqlglot transpilation — the LLM generates dbt-idiomatic SQL directly from the original proc statements, proc body, and profile answers.

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
3. discover show → statement breakdown → user reviews migrate/skip/claude; claude statements resolved via LLM + FDE confirmation; resolved statements written to catalog
4. profile       → catalog signals + LLM inference → user approves candidates
5. migrate       → generate dbt SQL (derives materialization + tests from profile) → user approves before file write
6. test-gen      → generate schema.yml + unit test fixtures → user approves before file write
7. validate      → compare outputs (skipped if no live DB)
```

**Gate rules:**

| After step | Gate |
|---|---|
| scope | Always — show writer list, user picks procedure |
| discover show | If `claude_assisted` — resolve via LLM, FDE confirms; write resolved statements to catalog |
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
| VU-773 | profile.py | VU-732 (shared lib), VU-733 (discover) |
| VU-774 | profile SKILL.md | VU-773 |
| VU-775 | profile tests | VU-773 |
| VU-776 | profiler-agent.md | VU-773 |
| VU-742 | migrate.py | VU-732, VU-773 |
| VU-743 | migrate SKILL.md | VU-742 |
| VU-744 | migrate tests | VU-742 |
| VU-777 | migrator-agent.md | VU-742 |

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
  ├── VU-773 (profile.py) ──── VU-774 (SKILL) ──── VU-775 (tests)
  │                        └── VU-776 (profiler-agent.md)
  │
  ├── VU-742 (migrate.py) ──── VU-743 (SKILL) ───── VU-744 (tests)
  │                        └── VU-777 (migrator-agent.md)
  │     │
  │     ├── VU-745 (test_gen.py) ── VU-746 (SKILL) ── VU-747 (tests)
  │     │
  │     └── VU-748 (validate.py) ── VU-749 (SKILL) ── VU-750 (tests)
  │
VU-751 (ddl_mcp) ✅

VU-739/740/741 (assess) cancelled — statement classification built into discover show.
VU-736/737/738 (scope.py) cancelled — scoping handled by scoping-agent.md calling discover refs + show.
VU-773 (profile.py) collects catalog signals; LLM inference is done by VU-776 (agent, batch) or VU-774 (skill, interactive).
All skills → VU-752 (migrate-table SKILL.md) → VU-753 (GHA workflow)
```

---

## Key Decisions

**sqlglot, not regex:** All parsing and analysis uses sqlglot AST. Unparseable procedures fail hard with `PARSE_FAILED` — no regex fallback.

**Deterministic tools, not LLM:** Scoping, assessment, and SQL transpilation have deterministic correct answers. Claude's role is judgment — reviewing output, handling transpile gaps, approving tests.

**Standalone scripts:** Each skill is independently testable and runnable from CLI. The orchestrator is a SKILL.md (plain text) and can be updated without touching Python.

**Validate is optional:** `validate.py` exits cleanly with `status: skipped` when `mssql_mcp` is absent.

**`ddl_mcp` shares `shared/`:** The MCP server and the skills both use `shared/loader.py` and `shared/name_resolver.py`. Same DDL files, no duplication.
