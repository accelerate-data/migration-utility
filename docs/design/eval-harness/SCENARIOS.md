# Eval Harness Scenarios

Use [README.md](README.md) for operator workflow.

Use [MAINTENANCE.md](MAINTENANCE.md) for harness maintenance workflow.

## Packages And Scenarios

A package is one runnable Promptfoo config.

A scenario is one test case inside a package YAML: one prompt, one fixture path, one set of vars, and one set of assertions.

Current package groups:

- Skill packages: `profiling-table`, `generating-tests`, `reviewing-tests`, `reviewing-model`, `analyzing-table`, `refactoring-sql`
- Command packages: `cmd-scope`, `cmd-profile`, `cmd-generate-model`, `cmd-generate-tests`, `cmd-refactor`, `cmd-status`, `cmd-commit-push-pr`
- Dialect packages: `oracle-regression`, `oracle-live`, `mssql-live`

There is a standalone `generating-tests` Promptfoo package in the current harness:

- `tests/evals/packages/generating-tests/skill-generating-tests.yaml`

There is still no standalone `generating-model` package. `generating-model` fixtures live under `tests/evals/fixtures/generating-model/` and are exercised via `cmd-generate-model`.

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
- `tests/evals/packages/generating-tests/skill-generating-tests.yaml`
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

## Ownership Notes

- `cmd-generate-model` owns the model-generation scenarios and planning-sweep scenarios.
- `generating-tests` owns the skill-level branch enumeration and fixture-synthesis scenarios.
- `cmd-generate-tests` owns command orchestration and review-loop behavior for test generation.
- View scenarios are handled by dedicated view prompts inside `profiling-table` and `analyzing-table`.
- Oracle regression covers `/scope`, `/profile`, `/generate-model`, `/generate-tests`, and `/refactor` against a shared SH-schema fixture.
