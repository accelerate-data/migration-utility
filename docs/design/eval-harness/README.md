# Eval Harness

Non-interactive test harness for skills and commands. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against pre-committed fixtures, then validates structured output.

---

## Architecture

```text
Offline regression (SQL Server + Oracle)
        │
        ▼  (fixtures pre-committed — no live DB needed)
Per-scenario fixtures (tests/evals/fixtures/<package>/<scenario-slug>/ or oracle-regression/fixtures/)
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
  .gitignore                           # node_modules/, output/
  assertions/
    schema-helpers.js                  # shared AJV validation, JSON extraction, term normalization
    check-procedure-catalog.js         # validates procedure catalog statements
    check-table-profile.js             # validates table profile classification
    check-table-scoping.js             # validates table scoping decisions
    check-view-catalog.js              # validates view scoping/profile catalog sections
    check-command-summary.js           # validates command orchestration summary
    check-status-output.js             # validates /status command output
    check-dbt-model.js                 # validates generated dbt model SQL
    check-dbt-refs.js                  # validates ref()/source() usage in generated dbt files
    check-model-generator-input.js     # validates model-generator input manifest
    check-refactored-sql.js            # validates refactored SQL extraction and CTE structure
    check-dbt-aware-refactored-sql.js  # validates dbt-aware refactor output against staging models
    check-test-spec.js                 # validates generated test specifications
    check-test-review.js               # validates test review results
    check-model-review.js              # validates model review results
    check-sweep-action.js              # validates planning-sweep skip/test-only behavior
    check-pr-safety.js                 # validates commit/push/PR failure handling
    validate-candidate-writers.js      # validates candidate writers against schema
  prompts/
    skill-profiling-table.txt          # prompt template for profiling-table skill
    skill-profiling-table-view.txt     # prompt template for profiling-table view scenarios
    skill-reviewing-tests.txt          # prompt template for reviewing-tests skill
    skill-reviewing-model.txt          # prompt template for reviewing-model skill
    skill-analyzing-table.txt          # prompt template for analyzing-table skill
    skill-analyzing-table-view.txt     # prompt template for analyzing-table view scenarios
    skill-refactoring-sql.txt          # prompt template for refactoring-sql skill
    cmd-scope.txt                      # prompt template for /scope command
    cmd-profile.txt                    # prompt template for /profile command
    cmd-generate-model.txt             # prompt template for /generate-model command
    cmd-generate-tests.txt             # prompt template for /generate-tests command
    cmd-refactor.txt                   # prompt template for /refactor command
    cmd-status.txt                     # prompt template for /status command
    cmd-commit-push-pr.txt             # prompt template for /commit-push-pr command
    cmd-live-pipeline.txt              # prompt template for live DB extract → scope → profile
  packages/                            # SQL Server offline packages (per-scenario fixtures)
    profiling-table/
      skill-profiling-table.yaml
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
    cmd-commit-push-pr/
      cmd-commit-push-pr.yaml
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
    <package>/                         # per-scenario fixture dirs
      <scenario-slug>/
        manifest.json
        ddl/procedures.sql             # only relevant procedure(s)
        catalog/tables/<table>.json
        catalog/procedures/<proc>.json
        dbt/...                        # only for generating/reviewing skills
        test-specs/<table>.json        # only for skills that need it
    planning-sweep-stg-reuse/          # synthetic: stg_product.sql pre-seeded on disk
    planning-sweep-no-stg/             # synthetic: empty staging dir — skill creates stg
    planning-sweep-shared-stg/         # synthetic: 2 tables sharing bronze.Product
    planning-sweep-skip/               # synthetic: planning sweep skip scenario
    planning-sweep-test-only/          # synthetic: planning sweep test-only scenario
    generating-model/                  # fixture families consumed by cmd-generate-model
    generating-tests/                  # fixture families consumed by cmd-generate-tests
```

---

## Packages

The harness is organized into package-local Promptfoo configs. Each package is self-contained and run individually; `npm run eval` chains all packages sequentially.

### Skill packages

Test individual skills in isolation (single-table, no orchestration). Each scenario uses its own per-scenario fixture under `fixtures/<package>/<scenario-slug>/`.

| Package | Skill |
|---|---|
| `profiling-table` | `/profiling-table` — includes view profiling scenarios |
| `reviewing-tests` | `/reviewing-tests` |
| `reviewing-model` | `/reviewing-model` |
| `analyzing-table` | `/analyzing-table` — validates both scoping decisions and procedure catalog; includes view scoping scenarios |
| `refactoring-sql` | `/refactoring-sql` — DML extraction + CTE restructuring |

There are no standalone `generating-model` or `generating-tests` Promptfoo packages in the current harness. Their scenario fixtures live under `tests/evals/fixtures/generating-model/` and `tests/evals/fixtures/generating-tests/`, but they are exercised via the command packages below.

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
| `cmd-commit-push-pr` | `/commit-push-pr` |

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

`npm run eval` runs the offline package chain defined in `package.json`. The Oracle regression and live DB packages are available as separate scripts and are not part of the default chain.

---

## Running evals

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals

# Install dependencies (first time only)
npm install

# All packages sequentially
npm run eval

# Single skill package
npm run eval:profiling-table
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
npm run eval:cmd-commit-push-pr

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

The package scripts restore their fixture roots before and after each run. Most offline package scripts reset `tests/evals/fixtures/`; the Oracle and live-DB scripts reset their package-local fixture roots; `eval:cmd-commit-push-pr` is text-only and does not restore fixtures because it does not operate on a fixture tree.

All eval scripts use `--no-cache` to force fresh LLM invocations.

---

## Scenario naming convention

Every scenario description follows the format:

```text
<statement-type-or-category> — <brief-detail>
```

The prefix before the em dash is either a SQL statement-type tag from the canonical taxonomy below, or a behavioral category prefix. This makes `--filter-pattern` a natural query language against description strings — filtering by prefix narrows to a statement type, filtering by suffix narrows to a specific behavior.

### Statement-type taxonomy (canonical)

These are the SQL pattern tags used as the primary prefix in scenario descriptions:

| Tag | SQL Pattern |
|---|---|
| `insert-select` | INSERT...SELECT full-refresh |
| `update-join` | UPDATE...FROM JOIN |
| `delete-where` | DELETE with WHERE |
| `delete-top` | DELETE TOP |
| `select-into` | SELECT INTO |
| `cte` | Single CTE |
| `correlated-subquery` | Correlated subquery |
| `union-all` | UNION ALL (bare) |
| `union-all-in-cte` | UNION ALL inside CTE |
| `intersect` | INTERSECT |
| `except` | EXCEPT |
| `grouping-sets` | GROUPING SETS |
| `cube` | CUBE |
| `rollup` | ROLLUP |
| `pivot` | PIVOT / conditional aggregation |
| `window-fn` | Window functions |
| `cross-join` | CROSS JOIN |
| `not-exists` | NOT EXISTS anti-join |
| `not-in` | NOT IN subquery |
| `recursive-cte` | Recursive CTE |
| `truncate-insert` | TRUNCATE + INSERT |
| `outer-apply` | OUTER APPLY |
| `merge` | MERGE INTO |
| `if-else` | IF/ELSE control flow |
| `try-catch` | BEGIN TRY/CATCH |
| `while-loop` | WHILE batch loop |
| `nested-flow` | Nested control flow |
| `exec-chain` | Static EXEC call chain |
| `exec-orchestrator` | EXEC-only orchestrator |
| `exec-variable` | EXEC(@sql) dynamic |
| `exec-concat` | EXEC string concatenation |
| `cross-db-exec` | EXEC with cross-database name |
| `linked-server-exec` | EXEC with linked server |
| `static-sp-exec` | sp_executesql with literal SQL |
| `dynamic-sql` | sp_executesql with variable SQL |
| `case-when` | CASE WHEN expressions |

### Behavioral category prefixes

For scenarios that don't test a specific SQL pattern:

| Prefix | When to use |
|---|---|
| `classification-*` | Profiling classification scenarios (dim-scd2, fact-periodic-snapshot, dim-junk) |
| `quality-*` | Code quality checks (style, ETL columns, YAML rendering, modular split) |
| `review-*` | Test/model review verdicts (approved, revision-requested, standards codes) |
| `pii` | PII detection |
| `watermark-*` | Watermark detection |
| `fk-*` | Foreign key type scenarios |
| `multi-writer` | Multi-writer disambiguation |
| `view-*` | View pipeline scenarios |
| `planning-sweep-*` | Planning sweep action scenarios |
| `idempotent-*` | Idempotency scenarios |
| `status-*` | Status command output scenarios |
| `happy-path` | Command happy path |
| `error-clean` | Command error + recovery |
| `guard-fail` | Command guard failure |
| `partial` | Command partial success |
| `review-cycle` | Command review loop |

### Filter examples

All commands assume you are in `tests/evals/`.

```bash
# All scenarios for a skill package
npm run eval:profiling-table

# Filter by statement type within the cmd-generate-tests package
npx promptfoo eval -c packages/cmd-generate-tests/cmd-generate-tests.yaml --filter-pattern "merge"

# Filter by behavioral category
npx promptfoo eval -c packages/reviewing-model/skill-reviewing-model.yaml --filter-pattern "review-standards"

# Combine skill + pattern
npx promptfoo eval -c packages/refactoring-sql/skill-refactoring-sql.yaml --filter-pattern "recursive-cte"
```

---

## Scenarios

The current scenario inventory lives in code, not in this document. Use the package YAMLs as the source of truth:

- `tests/evals/packages/profiling-table/skill-profiling-table.yaml`
- `tests/evals/packages/reviewing-tests/skill-reviewing-tests.yaml`
- `tests/evals/packages/reviewing-model/skill-reviewing-model.yaml`
- `tests/evals/packages/analyzing-table/skill-analyzing-table.yaml`
- `tests/evals/packages/refactoring-sql/skill-refactoring-sql.yaml`
- `tests/evals/packages/cmd-scope/cmd-scope.yaml`
- `tests/evals/packages/cmd-profile/cmd-profile.yaml`
- `tests/evals/packages/cmd-generate-model/cmd-generate-model.yaml`
- `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`
- `tests/evals/packages/cmd-refactor/cmd-refactor.yaml`
- `tests/evals/packages/cmd-status/cmd-status.yaml`
- `tests/evals/packages/cmd-commit-push-pr/cmd-commit-push-pr.yaml`
- `tests/evals/oracle-regression/promptfooconfig.yaml`
- `tests/evals/oracle-live/promptfooconfig.yaml`
- `tests/evals/mssql-live/promptfooconfig.yaml`

In the current harness:

- `cmd-generate-model` owns the model-generation scenarios and planning-sweep scenarios.
- `cmd-generate-tests` owns the test-generation scenarios, including idempotency coverage.
- View scenarios are handled by dedicated view prompts inside `profiling-table` and `analyzing-table`.
- Oracle regression covers `/scope`, `/profile`, `/generate-model`, `/generate-tests`, and `/refactor` against a shared SH-schema fixture.

### Command: profile

| Scenario | Tables | Key assertion |
|---|---|---|
| happy-path — both profile ok | InsertSelectTarget + UpdateJoinTarget | Summary shows 2 ok, `check-table-profile.js` validates artifact |
| error+clean — no scoping | DimProduct + InsertSelectTarget | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget still ok |

### Command: generate-model

| Scenario | Tables | Fixture | Key assertion |
|---|---|---|---|
| happy-path — both generate | InsertSelectTarget + UpdateJoinTarget | `migration-test` | Summary shows 2 ok, `check-dbt-model.js` validates artifact |
| review-revision cycle | CorrelatedSubqueryTarget | `migration-test` | Review loop invoked, final status ok |
| error+clean — no scoping | DimProduct + InsertSelectTarget | `migration-test` | SCOPING_NOT_COMPLETED for DimProduct, InsertSelectTarget ok |
| planning-sweep-shared-stg | InsertSelectTarget + SingleCteTarget | `planning-sweep-shared-stg` | Both SPs share `bronze.Product` — sweep creates ONE `stg_product.sql`, both marts ok |

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

Each scenario gets its own fixture directory under `fixtures/<package>/<scenario-slug>/`, containing only the files that scenario needs. This replaces the previous monolithic `fixtures/migration-test/` layout. A per-scenario fixture mirrors the production layout that `setup-ddl` produces, but scoped to the relevant procedure(s) and table(s):

```text
fixtures/<package>/<scenario-slug>/
  manifest.json                    # {"technology": "mssql", "dialect": "tsql", "database": "MigrationTest"}
  ddl/
    procedures.sql                 # only the procedure(s) relevant to this scenario
  catalog/
    tables/<schema>.<table>.json   # per-table catalog (columns, PKs, FKs, referenced_by, profile, scoping)
    procedures/<schema>.<proc>.json # per-proc catalog (references, statements)
  dbt/                             # only for generating/reviewing skills
    models/staging/                # pre-seeded dbt models and YAML configs
  test-specs/
    <schema>.<table>.json          # only for skills that need test specifications
```

Planning-sweep scenarios use synthetic fixtures (`planning-sweep-stg-reuse`, `planning-sweep-no-stg`, `planning-sweep-shared-stg`, `planning-sweep-skip`, `planning-sweep-test-only`) that differ only in which staging files are pre-seeded, allowing the eval to assert exactly which files the sweep creates or reuses. Synthetic fixtures are hand-crafted — do not re-extract them from the Docker database.

Per-scenario fixtures for downstream packages (`cmd-generate-model`, `cmd-generate-tests`, `reviewing-tests`, `reviewing-model`) include pre-seeded artifacts such as dbt models, test specs, and profiles so they can run without depending on upstream package output.

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

The exact extraction steps depend on how the migration project is configured.

---

## Maintaining scenarios

### Adding a scenario

1. **Create a per-scenario fixture** under `tests/evals/fixtures/<package>/<scenario-slug>/` with only the files the scenario needs (manifest, DDL, catalog, and optionally dbt/test-specs).
2. **Add a test case** to the appropriate package YAML.
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
        - Write              # omitted for review-only packages
        - Bash
        - Glob
        - Grep
```

- `working_dir: ../..` — repo root relative to `tests/evals/packages/<pkg>/`; for top-level packages (oracle-regression, oracle-live, mssql-live) the path is `../..` relative to their own directory
- `max_turns` — 80 for analyzing-table, cmd-scope, cmd-profile; 100 for cmd-generate-tests; 120 for cmd-generate-model, oracle-live, mssql-live; 60-70 for other skill packages
- `reviewing-tests` and `reviewing-model` omit Write from allowed tools (read-only review)

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
