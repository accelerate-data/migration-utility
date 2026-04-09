# Eval Harness Maintenance

Use [README.md](README.md) for day-to-day usage: what to run, how to narrow, and where to look when an eval fails.

Use this page when changing the harness itself: fixtures, prompts, assertions, package wiring, or provider configuration.

## Reference Layout

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
    <package>/
      <scenario-slug>/
        manifest.json
        ddl/procedures.sql
        catalog/tables/<table>.json
        catalog/procedures/<proc>.json
        dbt/...
        test-specs/<table>.json
    planning-sweep-stg-reuse/
    planning-sweep-no-stg/
    planning-sweep-shared-stg/
    planning-sweep-skip/
    planning-sweep-test-only/
    generating-model/
    generating-tests/
```

## Packages And Scenarios

A package is one runnable Promptfoo config.

A scenario is one test case inside a package YAML: one prompt, one fixture path, one set of vars, and one set of assertions.

Current package groups:

- Skill packages: `profiling-table`, `reviewing-tests`, `reviewing-model`, `analyzing-table`, `refactoring-sql`
- Command packages: `cmd-scope`, `cmd-profile`, `cmd-generate-model`, `cmd-generate-tests`, `cmd-refactor`, `cmd-status`, `cmd-commit-push-pr`
- Dialect packages: `oracle-regression`, `oracle-live`, `mssql-live`

There are no standalone `generating-model` or `generating-tests` Promptfoo packages in the current harness. Their fixtures live under `tests/evals/fixtures/generating-model/` and `tests/evals/fixtures/generating-tests/`, and are exercised via `cmd-generate-model` and `cmd-generate-tests`.

## Scenario Names

Scenario names are filterable labels stored in each YAML `description` field.

Examples:

```text
merge — DimProduct single MERGE writer
review-cycle — CorrelatedSubqueryTarget review loop
status-single-dimcurrency — catalog only, no scoping
```

Practical rule:

- Put the searchable part first.
- Use a short prefix that groups related tests.
- After the em dash, describe the specific case in plain English.

Common prefixes in the current harness include:

- SQL-pattern names such as `merge`, `insert-select`, `update-join`, `if-else`
- behavior names such as `happy-path`, `error-clean`, `review-cycle`, `guard-fail`
- feature areas such as `classification-*`, `view-*`, `planning-sweep-*`, `status-*`

The YAML files are the source of truth for scenario names.

## Scenario Inventory

Use these files as the source of truth:

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

## Writing Assertions

Each assertion is a custom JavaScript module that reads persisted artifacts and validates structural properties. Assertions use two layers:

- schema validation via AJV
- behavioral checks for expected values, terms, and cross-artifact consistency

### Schema Validation

Assertions validate JSON artifacts against schemas in `plugin/lib/shared/schemas/` using AJV Draft 2020-12. The shared `schema-helpers.js` module provides:

- `validateSchema(data, schemaFileName)`
- `validateSection(data, schemaFileName, sectionPath)`
- `extractJsonObject(output)`
- `normalizeTerms(value)`

Catalog assertions use section-level validation because fixtures may contain pre-existing data outside the section under test.

Review assertions and standalone schemas use full-schema validation because the entire JSON is produced in one pass.

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

### Cross-Artifact Consistency Checks

Some assertions verify stage handoff contracts:

- `check-table-profile.js`: `scoping.selected_writer` matches `profile.writer` when both exist
- `check-test-spec.js`: `branch_manifest[].scenarios` exist in `unit_tests[].name`; `item_id` matches `target_table`
- `check-test-review.js`: `item_id` matches `target_table`

### Assertion Pattern

All assertions are custom JS modules referenced from YAML:

```yaml
defaultTest:
  assert:
    - type: javascript
      value: file://../../assertions/check-procedure-catalog.js
```

Each module receives `(output, context)` and returns `{pass, score, reason}`.

## Fixture Anatomy

Each scenario gets its own fixture directory under `fixtures/<package>/<scenario-slug>/`, containing only the files that scenario needs.

```text
fixtures/<package>/<scenario-slug>/
  manifest.json
  ddl/
    procedures.sql
  catalog/
    tables/<schema>.<table>.json
    procedures/<schema>.<proc>.json
  dbt/
    models/staging/
  test-specs/
    <schema>.<table>.json
```

Planning-sweep scenarios use synthetic fixtures (`planning-sweep-stg-reuse`, `planning-sweep-no-stg`, `planning-sweep-shared-stg`, `planning-sweep-skip`, `planning-sweep-test-only`) that vary only in pre-seeded staging files.

Per-scenario fixtures for downstream packages (`cmd-generate-model`, `cmd-generate-tests`, `reviewing-tests`, `reviewing-model`) include pre-seeded artifacts such as dbt models, test specs, and profiles so they can run without depending on upstream package output.

## Extracting The Fixture

Run once after Docker MigrationTest is set up. Re-run only when `scripts/sql/create-migration-test-db.sql` changes.

```bash
docker ps | grep sql-test

export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=MigrationTest
export SA_PASSWORD=<your-password>

cd <migration-project-root>
claude --plugin-dir plugin/ -p "/setup-ddl"
uv run --project plugin/lib catalog-enrich
cp -r . tests/evals/fixtures/migration-test/
```

The exact extraction steps depend on how the migration project is configured.

## Maintaining Scenarios

### Adding a Scenario

1. Create a per-scenario fixture under `tests/evals/fixtures/<package>/<scenario-slug>/`.
2. Add a test case to the appropriate package YAML.
3. Run the affected package.

### Updating An Existing Scenario

1. Modify the fixture, prompt, assertion, or package vars.
2. Update YAML expectations if behavior changed.
3. Rerun the affected package or filtered scenario.

## Provider Configuration

Each package inlines the `anthropic:claude-agent-sdk` provider with package-specific `max_turns`:

```yaml
providers:
  - id: anthropic:claude-agent-sdk
    config:
      model: claude-sonnet-4-6
      working_dir: ../..
      max_turns: 80
      permission_mode: auto
      append_allowed_tools:
        - Read
        - Write
        - Bash
        - Glob
        - Grep
```

- `working_dir: ../..` is repo root relative to the package directory
- `max_turns` varies by package
- `reviewing-tests` and `reviewing-model` omit `Write`

## Cost Awareness

Each scenario invokes Claude with tool use. Command evals are typically more expensive than skill evals because they do more orchestration work.

Run the narrowest package or filtered scenario that answers the question you have.
