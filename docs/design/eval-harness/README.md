# Eval Harness

Non-interactive test harness for agents and skills. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) to invoke Claude Code in headless mode against the MigrationTest schema, then validates structured output and tool usage.

---

## Architecture

```text
MigrationTest schema (Docker SQL Server)
        │
        ▼  (one-time extraction via setup-ddl + catalog-enrich)
DDL project fixture (tests/evals/fixtures/migration-test/)
        │
        ▼  (Promptfoo invokes claude -p per scenario)
Claude Code headless ──► agents or skills ──► Python CLIs
        │
        ▼  (Promptfoo validates output)
Assertions: JSON schema, status enums, tool usage
```

Skills and agents share the same DDL project fixture but are tested separately because they have different execution paths:

| Dimension | Skill | Agent |
|---|---|---|
| Invocation | `/discover show --name <proc>` | Two file paths (input JSON, output JSON) |
| Output | Human-formatted text | Structured JSON |
| Approval gates | Interactive (simulated as non-interactive) | None |
| Assertions | Text sections, prose classifications | JSON schema validation, field values |

---

## Prerequisites

- Node.js (for `npx promptfoo`)
- `ANTHROPIC_API_KEY` in your shell environment
- DDL project fixture extracted from MigrationTest (see [Extracting the fixture](#extracting-the-fixture))

---

## Directory layout

```text
tests/evals/
  package.json                         # promptfoo + ajv
  .gitignore                           # node_modules/, output/
  promptfooconfig.yaml                 # full suite — imports all 3 packages
  providers/
    claude-code-headless.yaml          # exec provider wrapping claude -p
  prompts/
    skill-discover-show.txt            # prompt template for discover show skill
    agent-scoping.txt                  # prompt template for scoping agent
    skill-profile.txt                  # prompt template for profile skill
    agent-profiler.txt                 # prompt template for profiler agent
    skill-migrate.txt                  # prompt template for migrate skill
    agent-migrator.txt                 # prompt template for migrator agent
  packages/
    scoping/
      promptfooconfig.yaml             # package config — runs skill + agent
      skill-discover-show.yaml         # 8 skill scenarios
      agent-scoping.yaml               # 8 agent scenarios
    profiler/
      promptfooconfig.yaml
      skill-profile.yaml               # 4 skill scenarios
      agent-profiler.yaml              # 4 agent scenarios
    migrator/
      promptfooconfig.yaml
      skill-migrate.yaml               # 4 skill scenarios
      agent-migrator.yaml              # 4 agent scenarios
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
| `scoping` | `/discover show` on claude_assisted procs | scoping-agent | 8 x 2 = 16 |
| `profiler` | `/profile` (context + LLM reasoning + write) | profiler-agent | 4 x 2 = 8 |
| `migrator` | `/migrate` (context + dbt generation + write) | migrator-agent | 4 x 2 = 8 |

Each package has its own `promptfooconfig.yaml` that references the shared provider and the package's scenario files.

---

## Running evals

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals

# Install dependencies (first time only)
npm install

# Full suite — all packages
npx promptfoo eval

# Single package
npx promptfoo eval -c packages/scoping/promptfooconfig.yaml
npx promptfoo eval -c packages/profiler/promptfooconfig.yaml
npx promptfoo eval -c packages/migrator/promptfooconfig.yaml

# Single scenario file (skills only or agents only)
npx promptfoo eval -c packages/scoping/skill-discover-show.yaml
npx promptfoo eval -c packages/scoping/agent-scoping.yaml

# View results in browser
npx promptfoo view
```

`ANTHROPIC_API_KEY` must be in the environment. Promptfoo reads it automatically.

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

### Migrator scenarios

| Scenario ID | Target table | Key model assertion |
|---|---|---|
| `simple-insert-table` | silver.DimProduct | `materialized='table'` |
| `incremental-watermark` | silver.FactInternetSales | `is_incremental()` block |
| `snapshot-scd2` | (requires SCD2 fixture) | Snapshot config |
| `merge-incremental` | silver.DimProduct | Incremental + merge key |

---

## Writing assertions

### What to assert

- **Schema validity** — every agent output must validate against its JSON schema (via ajv)
- **Status enums** — `resolved`, `ambiguous_multi_writer`, `no_writer_found`, `partial`, `error`
- **Deterministic fields** — `selected_writer`, `summary.resolved`, `summary.error`
- **Tool usage** — agent called expected CLI commands (`discover refs`, `profile context`, etc.)
- **Section presence** (skills) — output contains Call Graph, Logic Summary, Migration Guidance

### What NOT to assert

- **Free-text rationale** — LLM wording varies between runs
- **Exact ordering** — of candidates, warnings, or dependency lists
- **Token-level output** — prompt phrasing changes cause harmless variation

### Assertion types

Promptfoo supports these assertion types in scenario YAML:

```yaml
assert:
  # JSON structure
  - type: is-json

  # Inline JavaScript — return true/false
  - type: javascript
    value: "JSON.parse(output).results[0].status === 'resolved'"

  # JSON schema validation
  - type: javascript
    value: |
      const Ajv = require('ajv');
      const schema = require('../../lib/shared/schemas/candidate_writers.json');
      return new Ajv().validate(schema, JSON.parse(output));

  # Text containment (for skill output or tool usage)
  - type: contains
    value: "Call Graph"

  # Negation
  - type: not-contains
    value: "error"
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

The scoping package uses the base extracted fixture. The profiler package needs the catalog enriched with resolved `statements` (post-scoping). The migrator package needs the catalog enriched with `profile` sections (post-profiling). These are layered snapshots stored as sibling directories if needed:

```text
fixtures/
  migration-test/                  # base extraction (scoping)
  migration-test-profiled/         # after scoping + profiling (migrator)
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
claude --plugin-dir . -p "/setup-ddl"

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
4. **Add a test case** to both `packages/scoping/skill-discover-show.yaml` and `packages/scoping/agent-scoping.yaml`:

```yaml
# In skill-discover-show.yaml
- description: "my-new-scenario — <what it tests>"
  vars:
    target_table: "silver.MyNewTable"
  assert:
    - type: contains
      value: "Call Graph"
    - type: contains
      value: "Migration Guidance"
    - type: contains
      value: "[migrate]"

# In agent-scoping.yaml
- description: "my-new-scenario — <what it tests>"
  vars:
    target_table: "silver.MyNewTable"
  assert:
    - type: is-json
    - type: javascript
      value: "JSON.parse(output).results[0].status === '<expected_status>'"
```

### Adding a profiler or migrator scenario

Same steps, but:

- The fixture must include the appropriate pipeline stage data (statements for profiler, profiles for migrator).
- Add the test case to the corresponding package's scenario files.
- Profiler assertions check `classification.resolved_kind`, `watermark`, `primary_key`, `pii_actions`.
- Migrator assertions check `materialization`, artifact paths, `dbt_compile_passed`.

### Updating an existing scenario

1. Modify the procedure in `create-migration-test-db.sql`.
2. Re-extract the fixture.
3. Update the assertions in the scenario YAML if the expected behavior changed.
4. Run the affected package: `npx promptfoo eval -c packages/<package>/promptfooconfig.yaml`.

### Refreshing the fixture after schema changes

```bash
# Re-run the SQL script against Docker
sqlcmd -S localhost,1433 -U sa -P '<password>' -d master -i scripts/sql/create-migration-test-db.sql

# Re-extract (see Extracting the fixture above)

# Verify scenarios still pass
cd tests/evals && npx promptfoo eval
```

---

## Provider configuration

The provider invokes Claude Code in headless mode:

```yaml
# providers/claude-code-headless.yaml
providers:
  - id: exec:claude-headless
    config:
      command: |
        claude -p \
          --output-format json \
          --allowedTools "Read,Write,Bash" \
          --max-turns 30 \
          --dangerously-skip-permissions \
          "{{prompt}}"
      timeout: 300000
```

- `--output-format json` — structured output for agent assertions
- `--allowedTools` — restricts to the tools agents/skills actually use
- `--max-turns 30` — matches agent maxTurns config
- `--dangerously-skip-permissions` — no interactive permission prompts
- Timeout: 5 minutes per scenario

---

## Cost awareness

Each scenario invokes Claude with tool use. At current pricing, expect roughly $0.10-0.50 per scenario depending on agent complexity and turn count.

- Full suite (32 scenarios): ~$5-15 per run
- Single package: ~$2-5 per run
- Single scenario: ~$0.10-0.50

Run selectively during development. Use `npx promptfoo eval -c packages/<package>/<file>.yaml` to test individual files.
