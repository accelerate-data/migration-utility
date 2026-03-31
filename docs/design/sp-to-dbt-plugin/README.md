# SP → dbt Migration Plugin

Deterministic Python skills that automate stored-procedure-to-dbt migration — discover, scope, profile, migrate, test-gen, and validate — using sqlglot AST analysis and LLM inference. No live database required for the deterministic steps. Claude orchestrates the skills and handles judgment calls.

---

## Scope

**In scope:** T-SQL stored procedures → dbt Spark SQL models (Fabric Lakehouse target).

**Out of scope:** bronze ingestion, ADF pipelines, Power BI, Fabric Lakehouse notebooks, Snowflake (planned extension).

---

## Where It Lives

The repo root is a Claude Code marketplace package. Four plugins are registered in `.claude-plugin/marketplace.json`.

```text
.claude-plugin/marketplace.json
├── bootstrap/                             ← plugin: init + setup + DDL extraction
│   ├── commands/                          ← init-ad-migration, init-dbt
│   └── skills/
│       └── setup-ddl/                    ← DDL extraction from live SQL Server
├── ground-truth-harness/                  ← plugin: sandbox + test generation + review
│   ├── commands/
│   │   ├── sandbox-up.md                 ← create throwaway DB + deploy DDL
│   │   └── sandbox-down.md               ← tear down throwaway DB
│   ├── agents/
│   │   ├── test-generator                ← batch: fixture synthesis + ground truth capture
│   │   └── test-reviewer                 ← batch: coverage scoring + quality gate
│   └── skills/
│       └── test-gen/                     ← interactive: /test-gen
├── migration/                             ← plugin: analysis + migration pipeline
│   ├── agents/
│   │   ├── scoping-agent.md
│   │   ├── migrator                      ← batch: dbt model generation + test loop
│   │   └── code-reviewer                 ← batch: standards + correctness gate
│   ├── skills/
│   │   ├── discover/                      ← SKILL.md + references/
│   │   ├── profile/
│   │   └── migrate/
│   └── commands/
│       └── migrate-table.md               ← orchestrator
├── mcp/
│   ├── ddl/                               ← DDL file MCP server
│   └── mssql/                             ← live SQL Server MCP config
├── lib/                                   ← Python package (uv-managed)
│   └── shared/                            ← Python modules (see Shared Library table)
│       └── schemas/                       ← JSON Schema files
```

Tests live at `tests/unit/`.

Each skill has a `SKILL.md` (Claude instructions) + a Python script in `lib/shared/`. The script outputs JSON to stdout. Claude runs it via `uv run`, reads the JSON, and decides next steps.

---

## Shared Library

All skills import from `shared/`. Nothing in `shared/` is skill-specific.

| Module | Responsibility |
|---|---|
| `loader.py` | Parse a DDL directory → `DdlCatalog` (GO-split + `sqlglot.parse_one`) |
| `loader_parse.py` | Body statement parsing, ref extraction, statement classification |
| `loader_data.py` | Data structures for DDL entries and object refs |
| `loader_io.py` | File I/O for DDL loading |
| `catalog.py` | Per-object catalog JSON file I/O: read/write `catalog/` files, routing flags, reference flipping |
| `catalog_dmf.py` | DMF result classification: group referenced entities by type, detect cross-database/cross-server refs |
| `catalog_enrich.py` | Offline AST enrichment: fill DMF gaps (SELECT INTO, TRUNCATE, EXEC chains) via sqlglot + BFS |
| `name_resolver.py` | Normalize FQN: strip brackets, lowercase, apply default schema |
| `env_config.py` | Environment and path resolution (project root, catalog dir, dbt project path) |
| `setup_ddl.py` | DDL setup orchestration: assemble modules, tables, manifest, and catalog from staging JSON |
| `discover.py` | CLI: list/show/refs/write-statements subcommands for DDL catalog queries |
| `profile.py` | CLI: context/write subcommands for profiling context assembly and catalog write-back |
| `migrate.py` | CLI: context/write subcommands for migration context assembly and dbt artifact generation |
| `dmf_processing.py` | DMF row processing helpers shared by catalog write paths |
| `sql_types.py` | SQL type mappings between T-SQL and target dialects |
| `test_harness.py` | CLI: sandbox-up/sandbox-down/execute subcommands for ground truth capture |

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

### Statement classification

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

### test-harness (CLI)

Three subcommands for sandbox lifecycle and ground truth capture:

#### `test-harness sandbox-up`

```text
Input:  sandbox-up --project-root PATH --run-id UUID

Output: { "database": "__test_<run_id>", "status": "ok", "tables_deployed": 12 }
```

Creates a throwaway database (`__test_<run_id>`), deploys table DDL from catalog with identical schema/table names. The proc runs unmodified — schema references resolve naturally within the sandbox namespace. Cross-platform via MCP: SQL Server (`CREATE DATABASE`), Snowflake (`CREATE DATABASE`/`CLONE`), Redshift (`CREATE SCHEMA`).

#### `test-harness execute`

```text
Input:  execute --project-root PATH --run-id UUID
        --writer dbo.usp_load_dimproduct
        --fixtures '{"given": [...], "target_table": "silver.dimproduct"}'

Output: { "rows": [{"product_key": 1, ...}], "row_count": 1, "status": "ok" }
```

Inserts fixture rows into source tables in the sandbox, executes the proc, captures output via `SELECT *` from the target table, cleans up inserted rows.

#### `test-harness sandbox-down`

```text
Input:  sandbox-down --run-id UUID

Output: { "database": "__test_<run_id>", "status": "dropped" }
```

Drops the throwaway database.

### test-gen (skill + agent)

Skill: `/test-gen` (interactive, user-invocable). Agent: `test-generator` (batch pipeline).

Both share the same Python CLI (`test_harness.py`) and `migrate context` for deterministic work. LLM reasoning (branch analysis, fixture synthesis) is path-specific.

```text
Input:  same as migrate context — --table <fqn> --writer <fqn>
        Sandbox must be running (sandbox-up).

Output: test-specs/<item_id>.json
```

See [Test Generator Agent Contract](../agent-contract/test-generator-agent.md) for the full output schema and generation strategy.

#### Shared between both paths

- **Interactive (`/test-gen` skill):** `migrate context` → LLM enumerates branches + synthesizes fixtures → `test-harness execute` per scenario → present test spec for approval → write to `test-specs/`.
- **Batch (test-generator agent):** `migrate context` per table → agent enumerates + synthesizes → `test-harness execute` per scenario → write to `test-specs/` → test-reviewer agent scores coverage and may kick back.

### validate (superseded)

The separate `validate.py` skill is superseded by the ground truth harness. Validation is now:

1. Test generator captures ground truth by executing the proc in a sandbox.
2. Migrator runs `dbt test` with `unit_tests:` (ground truth as expected output) against the generated model.

This replaces the prior approach of running both proc and model SQL and comparing result sets.

---

## `migrate-table` Command

The orchestrator. Defined in `commands/migrate-table.md`. No Python — Claude follows the instructions.

**Interactive flow:**

```text
1. discover       → list tables → user picks one
2. scope          → find writers → user confirms which procedure to migrate
3. discover show  → statement breakdown → user reviews migrate/skip/claude; resolved statements written to catalog
4. profile        → catalog signals + LLM inference → user approves candidates
5. sandbox-up     → create throwaway DB + deploy DDL (if not already running)
6. /test-gen      → synthesize fixtures, execute proc, capture ground truth → user approves test spec
7. /migrate       → generate dbt model + schema YAML (with unit_tests from test spec) → run dbt test → self-correct → user approves
8. sandbox-down   → tear down throwaway DB
```

**Gate rules:**

| After step | Gate |
|---|---|
| scope | Always — show writer list, user picks procedure |
| discover show | If `claude_assisted` — resolve via LLM, FDE confirms; write resolved statements to catalog |
| profile | Always — show classification, keys, watermark, PII candidates; user approves/edits |
| /test-gen | Always — show test spec (scenarios, coverage) before writing to `test-specs/` |
| /migrate | Always — show generated model before writing to disk |

**Batch (GHA) flow:**

```text
1. scoping-agent       → discover + resolve writers per table
2. profiler-agent      → profile per table
3. sandbox-up          → create throwaway DB
4. test-generator      → generate test specs per table
   test-reviewer       → score coverage, kick back if gaps (Loop 1)
5. migrator            → generate models, run dbt test, self-correct (Loop 2)
   code-reviewer       → check standards, kick back if issues
6. sandbox-down        → tear down
```

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

### Wave 3 — Profile + Migrate ✅

| Issue | What | Status |
|---|---|---|
| VU-773 | profile.py | Done |
| VU-774 | profile SKILL.md | Done |
| VU-775 | profile tests | Done |
| VU-776 | profiler-agent.md | Done |
| VU-742 | migrate.py | Done |
| VU-743 | migrate SKILL.md | Done |
| VU-744 | migrate tests | Done |
| VU-777 | migrator-agent.md | Done |

---

### Wave 4 — Ground Truth Harness + Test Generation

| Issue | What | Depends on |
|---|---|---|
| TBD | `test_harness.py` CLI (sandbox-up/down/execute) | VU-732, VU-742 |
| TBD | `ground-truth-harness` plugin scaffold | test_harness.py |
| TBD | sandbox-up.md + sandbox-down.md commands | plugin scaffold |
| VU-745 | test-generator agent contract | test_harness.py |
| VU-746 | /test-gen SKILL.md | VU-745 |
| VU-747 | test-gen tests | VU-745 |
| TBD | test-reviewer agent contract | VU-745 |
| TBD | code-reviewer agent contract | VU-777 |

**Exit criteria:** `test_harness.py` can create/destroy sandbox and capture ground truth. Test generator emits `test-specs/<item_id>.json` with `unit_tests[]`. Test reviewer scores coverage. Code reviewer reviews migrator output. VU-748/749/750 (validate) are superseded by the ground truth harness.

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
  │     ├── TBD (test_harness.py) ── TBD (ground-truth-harness plugin)
  │     │     ├── VU-745 (test-generator agent) ── VU-746 (/test-gen SKILL) ── VU-747 (tests)
  │     │     ├── TBD (test-reviewer agent)
  │     │     └── TBD (sandbox-up/down commands)
  │     │
  │     └── TBD (code-reviewer agent)
  │
  │  VU-748/749/750 (validate) — superseded by ground truth harness
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
