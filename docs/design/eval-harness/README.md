# Eval Harness

Non-interactive test harness for skills and commands. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against pre-committed fixtures, then validates structured output.

---

## Architecture

```text
Offline regression (SQL Server + Oracle)
        │
        ▼  (fixtures pre-committed — no live DB needed)
DDL project fixture (tests/evals/fixtures/migration-test/ or oracle-regression/fixtures/)
        │
        ▼  (Promptfoo invokes claude-agent-sdk per scenario)
Claude Code agent ──► reads fixture ──► calls Python CLIs ──► produces output
        │
        ▼  (Promptfoo validates output)
Assertions: custom JS validators check catalog JSON, dbt models, test specs, reviews

Live DB (optional — requires Docker)
        │
        ▼  (setup-ddl extract writes to fixture_path at runtime)
oracle-live / mssql-live packages — validate end-to-end extract → scope → profile
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
    schema-helpers.js                  # shared AJV validation, JSON extraction, term normalization
    check-procedure-catalog.js         # validates procedure catalog statements
    check-table-profile.js             # validates table profile classification
    check-dbt-model.js                 # validates generated dbt model SQL
    check-test-spec.js                 # validates generated test specifications
    check-test-review.js               # validates test review results
    check-model-review.js              # validates model review results
    check-table-scoping.js             # validates table scoping decisions
    check-command-summary.js           # validates command orchestration summary
    check-status-output.js             # validates /status command output
    check-model-generator-input.js     # validates model-generator input manifest
    check-refactored-sql.js            # validates refactored SQL extraction and CTE structure
    validate-candidate-writers.js      # validates candidate writers against schema
  prompts/
    skill-profiling-table.txt          # prompt template for profiling-table skill
    skill-generating-model.txt         # prompt template for generating-model skill
    skill-generating-tests.txt         # prompt template for generating-tests skill
    skill-reviewing-tests.txt          # prompt template for reviewing-tests skill
    skill-reviewing-model.txt          # prompt template for reviewing-model skill
    skill-analyzing-table.txt          # prompt template for analyzing-table skill
    skill-refactoring-sql.txt          # prompt template for refactoring-sql skill
    cmd-scope.txt                      # prompt template for /scope command
    cmd-profile.txt                    # prompt template for /profile command
    cmd-generate-model.txt             # prompt template for /generate-model command
    cmd-generate-tests.txt             # prompt template for /generate-tests command
    cmd-refactor.txt                   # prompt template for /refactor command
    cmd-live-pipeline.txt              # prompt template for live DB extract → scope → profile
  packages/                            # SQL Server offline packages (use fixtures/migration-test/)
    profiling-table/
      skill-profiling-table.yaml
    generating-model/
      skill-generating-model.yaml
    generating-tests/
      skill-generating-tests.yaml
    reviewing-tests/
      skill-reviewing-tests.yaml
    reviewing-model/
      skill-reviewing-model.yaml
    analyzing-table/
      skill-analyzing-table.yaml
    refactoring-sql/
      skill-refactoring-sql.yaml
    cmd-scope/
      cmd-scope.yaml
    cmd-profile/
      cmd-profile.yaml
    cmd-generate-model/
      cmd-generate-model.yaml
    cmd-generate-tests/
      cmd-generate-tests.yaml
    cmd-refactor/
      cmd-refactor.yaml
    cmd-status/
      cmd-status.yaml
  oracle-regression/                   # Oracle offline package (SH schema fixtures)
    promptfooconfig.yaml
    fixtures/                          # pre-committed Oracle SH schema fixtures
  oracle-live/                         # Oracle live DB package (requires Docker Oracle)
    promptfooconfig.yaml               # extract → scope → profile
    fixtures/manifest.json             # technology: oracle, dialect: oracle
  mssql-live/                          # SQL Server live DB package (requires Docker SQL Server)
    promptfooconfig.yaml               # extract → scope → profile
    fixtures/manifest.json             # technology: mssql, dialect: tsql
  fixtures/
    migration-test/                    # extracted DDL project (one-time, SQL Server)
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

Test individual skills in isolation (single-table, no orchestration). All use `fixtures/migration-test/` (SQL Server).

| Package | Skill |
|---|---|
| `profiling-table` | `/profiling-table` |
| `generating-model` | `/generating-model` |
| `generating-tests` | `/generating-tests` |
| `reviewing-tests` | `/reviewing-tests` |
| `reviewing-model` | `/reviewing-model` |
| `analyzing-table` | `/analyzing-table` — validates both scoping decisions and procedure catalog |
| `refactoring-sql` | `/refactoring-sql` — DML extraction + CTE restructuring |

### Command packages

Test batch command orchestration (multi-table dispatch, error handling, review loops, summary aggregation). Command evals suppress git operations and worktree creation — the agent operates directly on the fixture directory.

| Package | Command |
|---|---|
| `cmd-scope` | `/scope` |
| `cmd-profile` | `/profile` |
| `cmd-generate-model` | `/generate-model` |
| `cmd-generate-tests` | `/generate-tests` |
| `cmd-refactor` | `/refactor` |
| `cmd-status` | `/status` |

### Oracle regression package

Per-command Oracle dialect coverage using pre-committed SH schema fixtures. No live DB required.

| Package | Description |
|---|---|
| `oracle-regression` | scope / profile / generate-model / generate-tests / refactor against `SH.CHANNEL_SALES_SUMMARY` |

### Live DB packages

Validate the full extract → scope → profile pipeline against running Docker containers. Not run in CI — requires local Docker.

| Package | DB |
|---|---|
| `oracle-live` | Docker Oracle (FREEPDB1, SH schema) |
| `mssql-live` | Docker SQL Server (MigrationTest, silver schema) |

Use `promptfooconfig.yaml` for the full suite (skill scenarios only). Command and dialect-specific packages are run individually.

---

## Running evals

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals

# Install dependencies (first time only)
npm install

# Full skill suite — all SQL Server skill packages
npm run eval

# Single skill package
npm run eval:profiling-table
npm run eval:generating-model
npm run eval:generating-tests
npm run eval:reviewing-tests
npm run eval:reviewing-model
npm run eval:analyzing-table
npm run eval:refactoring-sql

# Command packages (run individually)
npm run eval:cmd-scope
npm run eval:cmd-profile
npm run eval:cmd-generate-model
npm run eval:cmd-generate-tests
npm run eval:cmd-refactor
npm run eval:cmd-status

# Oracle regression (offline, no live DB needed)
npm run eval:oracle-regression

# Live DB packages (require Docker)
npm run eval:oracle-live
npm run eval:mssql-live

# View results in browser
npm run view
```

`ANTHROPIC_API_KEY` must be in the environment. Promptfoo reads it automatically.

### Idempotency

Every npm eval script restores fixtures before and after each run (`git checkout -- fixtures/ && git clean -fd fixtures/`). This ensures each run starts from a clean state regardless of whether the previous run modified catalog files or dbt/test artifacts.

All eval scripts use `--no-cache` to force fresh LLM invocations.

---

## Scenarios

### Profiler

| Scenario | Target table | Writer | Key assertion |
|---|---|---|---|
| fact-rich-catalog | silver.FactInternetSales | usp_stage_FactInternetSales | fact_transaction classification |
| dim-merge | silver.DimProduct | usp_load_DimProduct | Valid classification kind |
| exec-call-chain | silver.FactExecProfile | usp_load_FactExecProfile | fact_transaction, status ok |
| cross-db-exec | silver.DimCrossDbProfile | usp_load_DimCrossDbProfile | Cross-database mention |
| dynamic-sp-executesql | silver.DimCurrency | usp_load_DimCurrency | ok/partial status, sp_executesql mention |

### Model-generator

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
| if-else | silver.IfElseTarget | IF/ELSE control flow decomposition |
| while-loop | silver.WhileLoopTarget | WHILE loop handling |
| static-sp-executesql | silver.StaticSpExecTarget | Static sp_executesql resolved |
| exec-variable-model | silver.ExecVariableTarget | EXEC(@sql) graceful no-model |
| exec-concat-model | silver.ExecConcatTarget | EXEC concat graceful no-model |

### Test-generator

| Scenario | Target table | Key assertion |
|---|---|---|
| merge-branches — DimProduct | silver.DimProduct | Branch manifest with multiple branches |
| exec-call-chain — FactInternetSales | silver.FactInternetSales | Transitive writer logic |
| dynamic-sql — DimCurrency | silver.DimCurrency | Dynamic SQL warning |
| delete-top — DeleteTopTarget | silver.DeleteTopTarget | Keep-vs-delete branches |
| recursive-cte — RecursiveCteTarget | silver.RecursiveCteTarget | Recursive anchor and step |
| try-catch — TryCatchTarget | silver.TryCatchTarget | TRY block as primary test target |
| nested-control-flow — NestedFlowTarget | silver.NestedFlowTarget | IF branches under TRY |

### Test-review

| Scenario | Target table | Key assertion |
|---|---|---|
| approved — DimProduct | silver.DimProduct | Approved with covered branches |
| revision-requested — DimCustomer | silver.DimCustomer | Revision requested |
| approved — InsertSelectTarget | silver.InsertSelectTarget | Approved, non-MERGE pattern |
| revision-requested — UpdateJoinTarget | silver.UpdateJoinTarget | Missing join-miss scenario |
| revision-requested — IfElseTarget | silver.IfElseTarget | Missing control-flow arm |
| approved — FactExecProfile | silver.FactExecProfile | Transitive writer chain covered |
| approved — DimCurrency | silver.DimCurrency | Partial coverage with untestable rationale |

### Code-review

| Scenario | Target table | Key assertion |
|---|---|---|
| approved — DimProduct MERGE | silver.DimProduct | Approved status |
| approved — InsertSelectTarget DML | silver.InsertSelectTarget | Correctness passed |
| revision-requested — DimCustomer | silver.DimCustomer | Flags issues |
| approved — UpdateJoinTarget | silver.UpdateJoinTarget | UPDATE→SELECT rewrite |
| approved — SelectIntoTarget | silver.SelectIntoTarget | Staging model materialization |
| approved — IfElseTarget | silver.IfElseTarget | Control-flow decomposition |
| approved — FactExecProfile | silver.FactExecProfile | Ref-only orchestrator |
| approved — DimCurrency | silver.DimCurrency | Dynamic SQL graceful degradation |

### Scoping-table

| Scenario | Target table | Expected writer |
|---|---|---|
| truncate-insert — DimCustomer | silver.DimCustomer | usp_load_DimCustomer_Full |
| merge — DimProduct | silver.DimProduct | usp_load_DimProduct |
| exec-orchestrator — FactInternetSales | silver.FactInternetSales | usp_load_FactInternetSales |
| cross-db-exec — DimCrossDbProfile | silver.DimCrossDbProfile | usp_load_DimCrossDbProfile |
| dynamic-sp-executesql — DimCurrency | silver.DimCurrency | usp_load_DimCurrency |
| exec-variable — ExecVariableTarget | silver.ExecVariableTarget | usp_scope_ExecVariable |
| exec-concat — ExecConcatTarget | silver.ExecConcatTarget | usp_scope_ExecConcat |
| linked-server-exec — LinkedServerExecTarget | silver.LinkedServerExecTarget | (no writer — skip) |

### Refactoring-sql

| Scenario | Target table | Pattern | Key assertion |
|---|---|---|---|
| insert-select | silver.InsertSelectTarget | INSERT...SELECT | Extracted SELECT + CTE refactored |
| merge-using | silver.DimProduct | MERGE USING | USING clause extracted, import CTEs |
| update-join | silver.UpdateJoinTarget | UPDATE SET | CASE expression in extraction |
| delete-where | silver.DeleteWhereTarget | DELETE WHERE | Inverted to keep-rows SELECT |
| truncate-insert | silver.DimCustomer | TRUNCATE+INSERT+OUTER APPLY | Full reload with MIN(OrderDate) |
| union-all | silver.UnionAllTarget | UNION ALL | Preserved in both outputs |
| window-functions | silver.FactInternetSales | Window functions | COUNT OVER preserved |
| grouping-sets | silver.GroupingSetsTarget | GROUPING SETS | Preserved in CTEs |
| pivot | silver.PivotTarget | PIVOT | Handled in CTE restructuring |
| delete-top | silver.DeleteTopTarget | DELETE TOP | Inverted to keep-rows SELECT |
| recursive-cte | silver.RecursiveCteTarget | Recursive CTE | UNION ALL anchor + step preserved |
| try-catch | silver.TryCatchTarget | TRY/CATCH | TRY block extracted, CATCH discarded |
| nested-control-flow | silver.NestedFlowTarget | Nested IF/TRY | Decomposed into CTE structure |

### Command: scope

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both resolve | DimProduct + DimCustomer | Summary shows 2 resolved, `check-table-scoping.js` validates artifact |
| error+clean — missing catalog | DimDate + DimProduct | CATALOG_FILE_MISSING for DimDate, DimProduct still resolves |

### Command: profile

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both profile ok | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok, `check-table-profile.js` validates artifact |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget still ok |

### Command: generate-model

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both generate | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok, `check-dbt-model.js` validates artifact |
| review-revision cycle | CorrelatedSubqueryTarget | Review loop invoked, final status ok |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget ok |

### Command: generate-tests

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both generate specs | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok |
| review-revision cycle | CorrelatedSubqueryTarget | Review loop invoked, final status ok |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget ok |

### Command: refactor

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both refactor | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok |
| error+clean — no profile | DimGeography + InsertSelectTarget | Guard error for DimGeography, ok for InsertSelectTarget |
| guard-fail — no scoping | DimPromotion | SCOPING_NOT_COMPLETED |
| partial-ok — dynamic SQL | DimCurrency | Partial status acceptable |

### Oracle regression

Per-command Oracle dialect coverage against `SH.CHANNEL_SALES_SUMMARY` written by `SH.SUMMARIZE_CHANNEL_SALES`. All catalog stages are pre-committed; each test is fully independent.

| Scenario | Command | Key assertion |
|---|---|---|
| scope — CHANNEL_SALES_SUMMARY resolves to SUMMARIZE_CHANNEL_SALES | `/scope` | `check-table-scoping.js` + `check-procedure-catalog.js` |
| profile — CHANNEL_SALES_SUMMARY profiles as fact_aggregate | `/profile` | `check-table-profile.js` |
| generate-model — CHANNEL_SALES_SUMMARY generates CTE dbt model | `/generate-model` | `check-dbt-model.js` |
| generate-tests — SUMMARIZE_CHANNEL_SALES enumerates PL/SQL branches | `/generate-tests` | `check-test-spec.js`, min 3 branches |
| refactor — CHANNEL_SALES_SUMMARY CTE restructured with final | `/refactor` | `check-refactored-sql.js` |

### Command: status

| Scenario | Target | Key assertion |
|---|---|---|
| single-table — InsertSelectTarget | silver.InsertSelectTarget | scope=resolved, profile=ok, test-gen blocked |
| single-table — DimCurrency | silver.DimCurrency | scope=pending, blocking profile |
| single-table — DimProduct | silver.DimProduct | scope blocking downstream |
| all-tables — summary | all catalog tables | stage names present across tables |

---

## Writing assertions

Each assertion is a custom JavaScript module that reads persisted artifacts (catalog JSON, test specs, dbt models) and validates structural properties. Assertions use a two-layer approach: **schema validation** (structural correctness via AJV) followed by **behavioral checks** (expected values, terms, cross-artifact consistency).

### Schema validation

Assertions validate JSON artifacts against schemas in `plugin/lib/shared/schemas/` using AJV (JSON Schema Draft 2020-12). The shared `schema-helpers.js` module provides:

- `validateSchema(data, schemaFileName)` — validates a full object against a named schema
- `validateSection(data, schemaFileName, sectionPath)` — validates a nested section against a `$defs` entry or property sub-schema
- `extractJsonObject(output)` — extracts JSON from LLM output text (fenced blocks or raw braces)
- `normalizeTerms(value)` — splits comma-separated strings into lowercase terms

Catalog assertions use **section-level validation** (not full-catalog validation) because fixtures contain pre-existing data that may not fully validate until the agent processes it. For example, `check-table-profile.js` validates only the `profile` section against the `profile_section` $def, not the entire table catalog.

Review assertions and standalone schemas use **full-schema validation** since the entire JSON is produced by the agent in a single pass.

| Assertion | Schema | Validation level |
|---|---|---|
| `check-table-scoping.js` | `table_catalog.json` | Section: `properties/scoping` |
| `check-table-profile.js` | `table_catalog.json` | Section: `profile_section` ($defs) |
| `check-procedure-catalog.js` | `procedure_catalog.json` | Section: `properties/statements` |
| `check-test-spec.js` | `test_spec.json` | Full schema |
| `check-model-review.js` | `model_review_output.json` | Full schema |
| `check-test-review.js` | `test_review_output.json` | Full schema |
| `check-command-summary.js` | `scoping_summary.json` | Full schema (when summary has `schema_version`) |
| `check-status-output.js` | `dry_run_output.json` | Full schema (per dry-run file) |
| `check-refactored-sql.js` | `table_catalog.json` | Section: `properties/refactor` |
| `check-model-generator-input.js` | `model_generator_input.json` | Full schema |
| `validate-candidate-writers.js` | `candidate_writers.json` | Full schema |

### Cross-artifact consistency checks

Some assertions verify that handoff contracts between pipeline stages are respected:

- **`check-table-profile.js`** — `scoping.selected_writer` matches `profile.writer` when both exist
- **`check-test-spec.js`** — `branch_manifest[].scenarios` entries exist in `unit_tests[].name`; `item_id` matches `target_table`
- **`check-test-review.js`** — `item_id` matches `target_table`

### What to assert

- **Schema conformance** — required fields, enum values, structural correctness (via AJV)
- **Behavioral correctness** — expected status, classification kind, term presence
- **Cross-artifact consistency** — handoff values match between pipeline stages
- **Model artifacts** — dbt SQL contains expected terms, forbidden terms absent
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

Each module receives `(output, context)` and returns `{pass, score, reason}`. Schema validation runs first as a gate — if the structure is invalid, behavioral checks are skipped.

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
docker ps | grep sql-test

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
      max_turns: 80          # varies: 60-120 depending on package
      permission_mode: auto
      append_allowed_tools:
        - Read
        - Write              # omitted for test-review and code-review
        - Bash
        - Glob
        - Grep
```

- `working_dir: ../..` — repo root relative to `tests/evals/packages/<pkg>/`; for top-level packages (oracle-regression, oracle-live, mssql-live) the path is `../..` relative to their own directory
- `max_turns` — 80 for analyzing-table, cmd-scope, cmd-profile; 100 for cmd-generate-tests; 120 for cmd-generate-model, oracle-live, mssql-live; 60-70 for other skill packages
- `test-review` and `code-review` omit Write from allowed tools (read-only review)

---

## Cost awareness

Each scenario invokes Claude with tool use. At current pricing, expect roughly $0.10-0.50 per scenario depending on agent complexity and turn count. Command evals are more expensive per scenario (~$0.50-1.00) due to multi-table orchestration and review loops.

- Full skill suite: ~$6-29 per run
- All command evals: ~$6-18 per run
- Oracle regression: ~$2-5 per run
- Live DB packages: ~$0.50-1.50 per run
- Single skill package: ~$1-5 per run
- Single command package: ~$1-3 per run

Run selectively during development. Use `npm run eval:<package>` to test a single package rather than the full suite. Command evals should be run from an isolated worktree to avoid branch-switching interference from other sessions. Live DB packages require Docker containers to be running.
