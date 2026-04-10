# Eval Harness Directory Layout

Use [README.md](README.md) for operator workflow.

Use [MAINTENANCE.md](MAINTENANCE.md) for harness maintenance workflow.

## Layout

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

  prompts/
    skill-profiling-table.txt          # prompt template for profiling-table skill
    skill-profiling-table-view.txt     # prompt template for profiling-table view scenarios
    skill-reviewing-tests.txt          # prompt template for reviewing-tests skill (tables)
    skill-reviewing-tests-view.txt     # prompt template for reviewing-tests skill (views)
    skill-reviewing-model.txt          # prompt template for reviewing-model skill
    skill-analyzing-table.txt          # prompt template for analyzing-table skill
    skill-analyzing-table-view.txt     # prompt template for analyzing-table view scenarios
    skill-refactoring-sql.txt          # prompt template for refactoring-sql skill
    skill-generating-tests.txt         # prompt template for generating-tests skill (tables)
    skill-generating-tests-view.txt    # prompt template for generating-tests skill (views)
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
    generating-tests/
      skill-generating-tests.yaml
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

## Reading The Layout

- `packages/` holds the main offline Promptfoo configs.
- `oracle-regression/`, `oracle-live/`, and `mssql-live/` hold dialect-specific packages.
- `fixtures/` holds per-scenario offline inputs.
- `prompts/` holds prompt templates used by the package YAMLs.
- `assertions/` holds the JS pass/fail checks.

When debugging a run, start with the package YAML, then the fixture, then the assertion, then the prompt.
