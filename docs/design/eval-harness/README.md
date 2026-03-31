# Eval Harness

Non-interactive test harness for agents and skills. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against the MigrationTest schema, then validates structured output.

---

## Architecture

```text
MigrationTest schema (Docker SQL Server)
        │
        ▼  (one-time extraction via setup-ddl + catalog-enrich)
DDL project fixture (tests/evals/fixtures/migration-test/)
        │
        ▼  (Promptfoo invokes claude-agent-sdk per scenario)
Claude Code agent ──► reads fixture ──► calls Python CLIs ──► produces output
        │
        ▼  (Promptfoo validates output)
Assertions: JSON schema (draft 2020-12), status enums, text sections
```

Skills and agents share the same DDL project fixture but are tested separately because they have different execution paths:

| Dimension | Skill | Agent |
|---|---|---|
| Invocation | `/discover-objects show --name <proc>` | Two file paths (input JSON, output JSON) |
| Output | Human-formatted text | Structured JSON |
| Approval gates | Interactive (simulated as non-interactive) | None |
| Assertions | `icontains` for text sections | JSON schema validation, field values |

---

## Prerequisites

- Node.js (for `npx promptfoo`)
- `ANTHROPIC_API_KEY` in your shell environment
- DDL project fixture extracted from MigrationTest (see [Extracting the fixture](#extracting-the-fixture))

---

## Directory layout

```text
tests/evals/
  package.json                         # promptfoo + ajv, npm run scripts
  .gitignore                           # node_modules/, output/
  assertions/
    validate-candidate-writers.js      # JSON schema validation (draft 2020-12)
  prompts/
    skill-discover-objects-show.txt     # prompt template for discover-objects show skill
    agent-scoping.txt                  # prompt template for scoping agent
    skill-profile-table.txt             # prompt template for profile-table skill
    agent-profiler.txt                 # prompt template for profiler agent
    skill-generate-model.txt            # prompt template for generate-model skill
    agent-model-generator.txt          # prompt template for model-generator agent
  packages/
    scoping/
      skill-discover-objects-show.yaml  # 8 skill scenarios (self-contained config)
      agent-scoping.yaml               # 8 agent scenarios (self-contained config)
    profiler/
      skill-profile-table.yaml          # 4 skill scenarios
      agent-profiler.yaml              # 4 agent scenarios
    model-generator/
      skill-generate-model.yaml        # 4 skill scenarios
      agent-model-generator.yaml       # 4 agent scenarios
  fixtures/
    migration-test/                    # extracted DDL project (one-time)
      manifest.json
      ddl/
      catalog/
    README.md                          # extraction instructions
```

---

## Packages

The harness is organized into three packages. Each package tests one stage of the migration pipeline and can be run independently.

| Package | Skills tested | Agent tested | Scenarios |
|---|---|---|---|
| `scoping` | `/discover-objects show` on claude_assisted procs | scoping-agent | 8 x 2 = 16 |
| `profiler` | `/profile-table` (context + LLM reasoning + write) | profiler-agent | 4 x 2 = 8 |
| `model-generator` | `/generate-model` (context + dbt generation + write) | model-generator-agent | 4 x 2 = 8 |

Each scenario file is a self-contained promptfoo config with its own `providers`, `prompts`, and `tests`. The npm scripts compose them via `-c` flags.

---

## Running evals

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals

# Install dependencies (first time only)
npm install

# Full suite — all packages
npm run eval

# Single package (skill + agent)
npm run eval:scoping
npm run eval:profiler
npm run eval:model-generator

# Single scenario file (skill only or agent only)
npm run eval:scoping:skill
npm run eval:scoping:agent

# View results in browser
npm run view
```

`ANTHROPIC_API_KEY` must be in the environment. Promptfoo reads it automatically.

### Idempotency

Every npm eval script restores fixtures before and after each run (`git checkout -- fixtures/ && git clean -fd fixtures/`). This ensures each run starts from a clean state regardless of whether the previous run modified catalog files (via `discover write-statements`, `profile write`, etc.).

All eval scripts use `--no-cache` to force fresh LLM invocations.

---

## Scenarios

### Scoping scenarios (from MigrationTest)

Each scenario targets a table from the MigrationTest schema. The expected status is determined by the procedures that write to that table.

| Scenario ID | Target table | Procedure(s) | Expected status |
|---|---|---|---|
| `resolved` | silver.DimProduct | usp_load_DimProduct | `resolved` |
| `ambiguous-multi-writer` | silver.DimCustomer | usp_load_DimCustomer_Full + Delta | `ambiguous_multi_writer` |
| `call-graph` | silver.FactInternetSales | usp_load → usp_stage | `resolved` |
| `no-writer-found` | silver.DimGeography | (none) | `no_writer_found` |
| `partial` | silver.DimCurrency | usp_load_DimCurrency | `partial` |
| `error-cross-db` | silver.DimEmployee | usp_load_DimEmployee | `error` |
| `writer-through-view` | silver.DimPromotion | usp_load_DimPromotion | `resolved` |
| `mv-as-target` | silver.DimSalesTerritory | (TBD) | `resolved` |

### Profiler scenarios

| Scenario ID | Target table | Key LLM inference |
|---|---|---|
| `fact-rich-catalog` | silver.FactInternetSales | `fact_transaction`, watermark, PK from catalog |
| `dim-scd2` | (requires SCD2 fixture) | `dim_scd2`, date columns |
| `dim-full-reload` | silver.DimProduct | `dim_full_reload`, null watermark |
| `pii-detection` | (requires PII fixture) | PII actions populated |

### Model-generator scenarios

| Scenario ID | Target table | Key model assertion |
|---|---|---|
| `simple-insert-table` | silver.DimProduct | `materialized='table'` |
| `incremental-watermark` | silver.FactInternetSales | `is_incremental()` block |
| `snapshot-scd2` | (requires SCD2 fixture) | Snapshot config |
| `merge-incremental` | silver.DimProduct | Incremental + merge key |

---

## Writing assertions

### What to assert

- **Schema validity** — agent output validated against JSON schema (draft 2020-12) via `assertions/validate-candidate-writers.js`
- **Status enums** — `resolved`, `ambiguous_multi_writer`, `no_writer_found`, `error`
- **Deterministic fields** — `selected_writer`, `summary.resolved`, `summary.error`
- **Section presence** (skills) — output contains Call Graph, Logic Summary, Migration Guidance

### What NOT to assert

- **Free-text rationale** — LLM wording varies between runs
- **Exact ordering** — of candidates, warnings, or dependency lists
- **Token-level output** — prompt phrasing changes cause harmless variation
- **Tool usage** — tool calls are in SDK metadata, not in the output text

### Assertion patterns

**Agent scenarios** use `options.transform` to extract JSON from conversational output, then validate:

```yaml
defaultTest:
  options:
    transform: "(output.match(/\\{[\\s\\S]*\"schema_version\"...) || [output])[0]"
  assert:
    # JSON schema validation (draft 2020-12, uses Ajv2020 + ajv-formats)
    - type: javascript
      value: file://../../assertions/validate-candidate-writers.js
tests:
  - description: "resolved — DimProduct"
    assert:
      - type: javascript
        value: "JSON.parse(output).results[0].status === 'resolved'"
```

**Skill scenarios** use case-insensitive text containment:

```yaml
defaultTest:
  assert:
    - type: icontains
      value: "Call Graph"
    - type: icontains
      value: "Logic Summary"
tests:
  - description: "resolved — DimProduct"
    assert:
      - type: icontains
        value: "Migration Guidance"
      - type: icontains
        value: "MERGE"
```

---

## Fixture anatomy

The fixture is a DDL project extracted from the MigrationTest Docker database. It mirrors the production layout that `setup-ddl` produces:

```text
fixtures/migration-test/
  manifest.json                    # {"technology": "mssql", "dialect": "tsql", "database": "MigrationTest"}
  ddl/
    tables.sql                     # CREATE TABLE statements (bronze + silver)
    procedures.sql                 # CREATE PROCEDURE statements
    views.sql                      # CREATE VIEW statements
  catalog/
    tables/<schema>.<table>.json   # per-table catalog (columns, PKs, FKs, referenced_by)
    procedures/<schema>.<proc>.json # per-proc catalog (references, statements)
    views/<schema>.<view>.json     # per-view catalog
```

The scoping package uses the base extracted fixture. The profiler package needs the catalog enriched with resolved `statements` (post-scoping). The model-generator package needs the catalog enriched with `profile` sections (post-profiling). These are layered snapshots stored as sibling directories if needed:

```text
fixtures/
  migration-test/                  # base extraction (scoping)
  migration-test-profiled/         # after scoping + profiling (model-generator)
```

---

## Extracting the fixture

Run once after Docker MigrationTest is set up. Re-run only when `scripts/sql/create-migration-test-db.sql` changes.

```bash
# 1. Ensure Docker container is running with MigrationTest loaded
docker ps | grep aw-sql

# 2. Set environment variables for MCP connection
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=MigrationTest
export SA_PASSWORD=<your-password>

# 3. Run setup-ddl to extract DDL and build catalog
#    (from a project directory pointed at MigrationTest)
cd <migration-project-root>
claude --plugin-dir plugins/ -p "/setup-ddl"

# 4. Run catalog-enrich for AST enrichment
uv run --project lib catalog-enrich

# 5. Copy the extracted project to the fixture directory
cp -r . tests/evals/fixtures/migration-test/
```

The exact extraction steps depend on how the migration project is configured. See `docs/reference/local-agent-testing/` for the full setup guide.

---

## Maintaining scenarios

### Adding a new scoping scenario

1. **Add the procedure and target table** to `scripts/sql/create-migration-test-db.sql`.
2. **Rebuild the Docker image** and re-run `create-migration-test-db.sql` against it.
3. **Re-extract the fixture** (see [Extracting the fixture](#extracting-the-fixture)).
4. **Add a test case** to both `packages/scoping/skill-discover-objects-show.yaml` and `packages/scoping/agent-scoping.yaml`:

```yaml
# In skill-discover-objects-show.yaml
- description: "my-new-scenario — <what it tests>"
  vars:
    target_table: "silver.MyNewTable"
  assert:
    - type: icontains
      value: "Migration Guidance"

# In agent-scoping.yaml
- description: "my-new-scenario — <what it tests>"
  vars:
    target_table: "silver.MyNewTable"
  assert:
    - type: javascript
      value: "JSON.parse(output).results[0].status === '<expected_status>'"
```

### Adding a profiler or model-generator scenario

Same steps, but:

- The fixture must include the appropriate pipeline stage data (statements for profiler, profiles for model-generator).
- Add the test case to the corresponding package's scenario files.
- Profiler assertions check `classification.resolved_kind`, `watermark`, `primary_key`, `pii_actions`.
- Model-generator assertions check `materialization`, artifact paths, `dbt_compile_passed`.

### Updating an existing scenario

1. Modify the procedure in `create-migration-test-db.sql`.
2. Re-extract the fixture.
3. Update the assertions in the scenario YAML if the expected behavior changed.
4. Run the affected package: `npm run eval:scoping` (or `eval:profiler`, `eval:model-generator`).

### Refreshing the fixture after schema changes

```bash
# Re-run the SQL script against Docker
sqlcmd -S localhost,1433 -U sa -P '<password>' -d master -i scripts/sql/create-migration-test-db.sql

# Re-extract (see Extracting the fixture above)

# Verify scenarios still pass
cd tests/evals && npm run eval
```

---

## Provider configuration

Each scenario file inlines the `anthropic:claude-agent-sdk` provider:

```yaml
providers:
  - id: anthropic:claude-agent-sdk
    config:
      model: claude-sonnet-4-6
      working_dir: ../..
      max_turns: 30
      permission_mode: bypassPermissions
      allow_dangerously_skip_permissions: true
      append_allowed_tools:
        - Read
        - Write
        - Bash
        - Glob
        - Grep
```

- `model` — matches agent definitions (sonnet for evals, override per scenario if needed)
- `working_dir: ../..` — repo root relative to `tests/evals/`, gives the agent filesystem access
- `max_turns: 30` — matches agent maxTurns config
- `permission_mode: bypassPermissions` — no interactive permission prompts
- `append_allowed_tools` — Read, Write, Bash, Glob, Grep (the tools agents/skills use)

---

## Cost awareness

Each scenario invokes Claude with tool use. At current pricing, expect roughly $0.10-0.50 per scenario depending on agent complexity and turn count.

- Full suite (32 scenarios): ~$5-15 per run
- Single package: ~$2-5 per run
- Single scenario: ~$0.10-0.50

Run selectively during development. Use `npm run eval:scoping:agent` to test a single file rather than the full suite.
