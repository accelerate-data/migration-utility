# Eval Harness

Non-interactive test harness for skills and commands. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against the MigrationTest schema, then validates structured output.

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
Assertions: custom JS validators check catalog JSON, dbt models, test specs, reviews
```

Skills are tested via Promptfoo scenarios that invoke them non-interactively and validate their output. Commands (multi-table batch orchestrators) are tested via separate command eval packages that exercise parallel dispatch, error handling, review loops, and summary aggregation.

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
  promptfooconfig.yaml                 # aggregate suite config for `npx promptfoo eval`
  .gitignore                           # node_modules/, output/
  assertions/
    check-procedure-catalog.js         # validates procedure catalog statements
    check-table-profile.js             # validates table profile classification
    check-dbt-model.js                 # validates generated dbt model SQL
    check-test-spec.js                 # validates generated test specifications
    check-test-review.js               # validates test review results
    check-model-review.js              # validates model review results
    check-table-scoping.js             # validates table scoping decisions
    check-command-summary.js           # validates command orchestration summary
  prompts/
    skill-profiling-table.txt          # prompt template for profiling-table skill
    skill-generating-model.txt         # prompt template for generating-model skill
    skill-generating-tests.txt         # prompt template for generating-tests skill
    skill-reviewing-tests.txt          # prompt template for reviewing-tests skill
    skill-reviewing-model.txt          # prompt template for reviewing-model skill
    skill-scoping-table.txt            # prompt template for scoping-table skill
    cmd-scope.txt                      # prompt template for /scope command
    cmd-profile.txt                    # prompt template for /profile command
    cmd-generate-model.txt             # prompt template for /generate-model command
    cmd-generate-tests.txt             # prompt template for /generate-tests command
  packages/
    profiler/
      skill-profiling-table.yaml       # 4 scenarios
    model-generator/
      skill-generating-model.yaml      # 16 scenarios
    test-generator/
      skill-generating-tests.yaml      # 3 scenarios
    test-review/
      skill-reviewing-tests.yaml       # 2 scenarios
    code-review/
      skill-reviewing-model.yaml       # 3 scenarios
    scoping-table/
      skill-scoping-table.yaml         # 7 scenarios
    cmd-scope/
      cmd-scope.yaml                   # 2 scenarios
    cmd-profile/
      cmd-profile.yaml                 # 2 scenarios
    cmd-generate-model/
      cmd-generate-model.yaml          # 3 scenarios
    cmd-generate-tests/
      cmd-generate-tests.yaml          # 3 scenarios
  fixtures/
    migration-test/                    # extracted DDL project (one-time)
      manifest.json
      ddl/
      catalog/
      dbt/                             # pre-seeded dbt models and test specs
      test-specs/                      # pre-seeded test specifications
```

---

## Packages

The harness is organized into package-local Promptfoo configs plus one aggregate top-level config.

### Skill packages

Test individual skills in isolation (single-table, no orchestration).

| Package | Skill | Scenarios |
|---|---|---|
| `profiler` | `/profiling-table` | 4 |
| `model-generator` | `/generating-model` | 16 |
| `test-generator` | `/generating-tests` | 3 |
| `test-review` | `/reviewing-tests` | 2 |
| `code-review` | `/reviewing-model` | 3 |
| `scoping-table` | `/scoping-table` | 7 (validates both scoping decisions and procedure catalog) |

### Command packages

Test batch command orchestration (multi-table dispatch, error handling, review loops, summary aggregation). Command evals suppress git operations and worktree creation — the agent operates directly on the fixture directory.

| Package | Command | Scenarios |
|---|---|---|
| `cmd-scope` | `/scope` | 2 |
| `cmd-profile` | `/profile` | 2 |
| `cmd-generate-model` | `/generate-model` | 3 |
| `cmd-generate-tests` | `/generate-tests` | 3 |

Use `promptfooconfig.yaml` for the full suite (skill scenarios only). Command packages are run individually via `npm run eval:cmd-*`.

---

## Running evals

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals

# Install dependencies (first time only)
npm install

# Full skill suite — all skill packages (35 scenarios)
npm run eval

# Single skill package
npm run eval:profiler
npm run eval:model-generator
npm run eval:test-generator
npm run eval:test-review
npm run eval:code-review
npm run eval:scoping-table

# Command packages (10 scenarios total, run individually)
npm run eval:cmd-scope
npm run eval:cmd-profile
npm run eval:cmd-generate-model
npm run eval:cmd-generate-tests

# View results in browser
npm run view
```

`ANTHROPIC_API_KEY` must be in the environment. Promptfoo reads it automatically.

### Idempotency

Every npm eval script restores fixtures before and after each run (`git checkout -- fixtures/ && git clean -fd fixtures/`). This ensures each run starts from a clean state regardless of whether the previous run modified catalog files or dbt/test artifacts.

All eval scripts use `--no-cache` to force fresh LLM invocations.

---

## Scenarios

### Profiler (4 scenarios)

| Scenario | Target table | Writer | Key assertion |
|---|---|---|---|
| fact-rich-catalog | silver.FactInternetSales | usp_stage_FactInternetSales | fact_transaction classification |
| dim-merge | silver.DimProduct | usp_load_DimProduct | Valid classification kind |
| exec-call-chain | silver.FactExecProfile | usp_load_FactExecProfile | fact_transaction, status ok |
| cross-db-exec | silver.DimCrossDbProfile | usp_load_DimCrossDbProfile | Cross-database mention |

### Model-generator (16 scenarios)

| Scenario | Target table | Pattern |
|---|---|---|
| insert-select | silver.InsertSelectTarget | Simple INSERT...SELECT |
| update-join | silver.UpdateJoinTarget | UPDATE with join |
| delete-where | silver.DeleteWhereTarget | DELETE with WHERE (graceful no-model) |
| select-into | silver.SelectIntoTarget | SELECT INTO |
| single-cte | silver.SingleCteTarget | CTE restructured |
| correlated-subquery | silver.CorrelatedSubqueryTarget | Correlated subquery |
| union-all | silver.UnionAllTarget | UNION ALL preserved |
| grouping-sets | silver.GroupingSetsTarget | GROUPING SETS / GROUP BY |
| pivot | silver.PivotTarget | PIVOT to conditional aggregation |
| window-functions | silver.FactInternetSales | Window expressions preserved |
| outer-apply | silver.DimCustomer | OUTER APPLY rewrite |
| truncate-insert | silver.DimCustomer | Full-reload materialization |
| exec-orchestrator | silver.FactInternetSales | Orchestration (graceful no-model) |
| dynamic-sp-executesql | silver.DimCurrency | Dynamic SQL (graceful no-model) |

### Test-generator (3 scenarios)

| Scenario | Target table | Key assertion |
|---|---|---|
| merge-branches — DimProduct | silver.DimProduct | Branch manifest with 5+ branches |
| exec-call-chain — FactInternetSales | silver.FactInternetSales | Transitive writer logic |
| dynamic-sql — DimCurrency | silver.DimCurrency | Dynamic SQL warning |

### Test-review (2 scenarios)

| Scenario | Target table | Key assertion |
|---|---|---|
| approved — DimProduct | silver.DimProduct | Approved with covered branches |
| revision-requested — DimCustomer | silver.DimCustomer | Revision requested |

### Code-review (3 scenarios)

| Scenario | Target table | Key assertion |
|---|---|---|
| approved — DimProduct MERGE | silver.DimProduct | Approved status |
| approved — InsertSelectTarget DML | silver.InsertSelectTarget | Correctness passed |
| revision-requested — DimCustomer | silver.DimCustomer | Flags issues |

### Scoping-table (7 scenarios)

| Scenario | Target table | Expected writer |
|---|---|---|
| truncate-insert — DimCustomer | silver.DimCustomer | usp_load_DimCustomer_Full |
| merge — DimProduct | silver.DimProduct | usp_load_DimProduct |
| exec-orchestrator — FactInternetSales | silver.FactInternetSales | usp_load_FactInternetSales |
| cross-db-exec — DimCrossDbProfile | silver.DimCrossDbProfile | usp_load_DimCrossDbProfile |
| dynamic-sp-executesql — DimCurrency | silver.DimCurrency | usp_load_DimCurrency |
| exec-variable — ExecVariableTarget | silver.ExecVariableTarget | usp_scope_ExecVariable |
| exec-concat — ExecConcatTarget | silver.ExecConcatTarget | usp_scope_ExecConcat |

### Command: scope (2 scenarios)

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both resolve | DimProduct + DimCustomer | Summary shows 2 resolved, `check-table-scoping.js` validates artifact |
| error+clean — missing catalog | DimDate + DimProduct | CATALOG_FILE_MISSING for DimDate, DimProduct still resolves |

### Command: profile (2 scenarios)

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both profile ok | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok, `check-table-profile.js` validates artifact |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget still ok |

### Command: generate-model (3 scenarios)

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both generate | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok, `check-dbt-model.js` validates artifact |
| review-revision cycle | CorrelatedSubqueryTarget | Review loop invoked, final status ok |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget ok |

### Command: generate-tests (3 scenarios)

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both generate specs | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok |
| review-revision cycle | CorrelatedSubqueryTarget | Review loop invoked, final status ok |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget ok |

---

## Writing assertions

Each assertion is a custom JavaScript module that reads persisted artifacts (catalog JSON, test specs, dbt models) and validates structural properties.

### What to assert

- **Catalog structure** — statements exist, source fields set, status enums correct
- **Profile classification** — `resolved_kind` is a valid enum, source is `catalog`/`llm`/`catalog+llm`
- **Model artifacts** — dbt SQL contains expected terms, forbidden terms absent
- **Test specs** — branch counts, scenario counts, status/coverage enums
- **Review results** — status enum matches expected range
- **Scoping decisions** — writer selected, status resolved
- **Command summaries** — per-item statuses in output text, error codes present for failed items

### What NOT to assert

- **Free-text rationale** — LLM wording varies between runs
- **Exact ordering** — of candidates, warnings, or dependency lists
- **Exact status for review evals** — accept comma-separated status ranges (e.g., `approved,approved_with_warnings`)
- **Token-level output** — prompt phrasing changes cause harmless variation

### Assertion pattern

All assertions follow the same pattern — custom JS modules referenced from YAML:

```yaml
defaultTest:
  assert:
    - type: javascript
      value: file://../../assertions/check-procedure-catalog.js
```

Each module receives `(output, context)` and returns `{pass, score, reason}`.

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
    tables/<schema>.<table>.json   # per-table catalog (columns, PKs, FKs, referenced_by, profile, scoping)
    procedures/<schema>.<proc>.json # per-proc catalog (references, statements)
    views/<schema>.<view>.json     # per-view catalog
  dbt/
    models/staging/                # pre-seeded dbt models and YAML configs
  test-specs/
    <schema>.<table>.json          # pre-seeded test specifications
```

All packages use the same fixture directory. The fixture includes pre-seeded artifacts (dbt models, test specs, profiles) so that downstream evals (test-generator, test-review, code-review) can run without depending on upstream skill output.

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
cd <migration-project-root>
claude --plugin-dir plugin/ -p "/setup-ddl"

# 4. Run catalog-enrich for AST enrichment
uv run --project plugin/lib catalog-enrich

# 5. Copy the extracted project to the fixture directory
cp -r . tests/evals/fixtures/migration-test/
```

The exact extraction steps depend on how the migration project is configured. See `docs/reference/local-agent-testing/` for the full setup guide.

---

## Maintaining scenarios

### Adding a scenario

1. **Add the procedure and target table** to `tests/evals/fixtures/migration-test/ddl/procedures.sql` and corresponding catalog JSON.
2. **Add a test case** to the appropriate package YAML and to `promptfooconfig.yaml`.
3. **Run the affected package** to verify: `npm run eval:<package>`.

### Updating an existing scenario

1. Modify the fixture (catalog JSON, DDL, dbt models, test specs).
2. Update assertions in the YAML if expected behavior changed.
3. Run the affected package.

---

## Provider configuration

Each package inlines the `anthropic:claude-agent-sdk` provider with package-specific `max_turns`:

```yaml
providers:
  - id: anthropic:claude-agent-sdk
    config:
      model: claude-sonnet-4-6
      working_dir: ../..
      max_turns: 80          # varies: 60-80 depending on package
      permission_mode: bypassPermissions
      allow_dangerously_skip_permissions: true
      append_allowed_tools:
        - Read
        - Write              # omitted for test-review and code-review
        - Bash
        - Glob
        - Grep
```

- `working_dir: ../..` — repo root relative to `tests/evals/packages/<pkg>/`
- `max_turns` — 80 for scoping-table, cmd-scope, cmd-profile; 100 for cmd-generate-tests; 120 for cmd-generate-model; 60-70 for other skill packages
- `test-review` and `code-review` omit Write from allowed tools (read-only review)

---

## Cost awareness

Each scenario invokes Claude with tool use. At current pricing, expect roughly $0.10-0.50 per scenario depending on agent complexity and turn count. Command evals are more expensive per scenario (~$0.50-1.00) due to multi-table orchestration and review loops.

- Full skill suite (35 scenarios): ~$5-18 per run
- All command evals (10 scenarios): ~$5-10 per run
- Single skill package: ~$1-5 per run
- Single command package: ~$1-3 per run

Run selectively during development. Use `npm run eval:<package>` to test a single package rather than the full suite. Command evals should be run from an isolated worktree to avoid branch-switching interference from other sessions.
