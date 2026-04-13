# Eval Harness Maintenance

Use [README.md](README.md) for day-to-day usage: what to run, how to narrow, and where to look when an eval fails.

Use this page when changing the harness itself: fixtures, prompts, assertions, package wiring, or provider configuration.

Reference pages:

- [DIRECTORY.md](DIRECTORY.md) for the filesystem layout
- [SCENARIOS.md](SCENARIOS.md) for package ownership and scenario inventory

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

All output shape enforcement is handled by Pydantic models (`extra="forbid"`) in `output_models.py` and `catalog_models.py`. JSON Schema files and AJV validation have been removed. JS assertions now focus on cross-artifact consistency and expected-term matching only.

| Assertion | What it checks |
|---|---|
| `check-table-scoping.js` | Scoping section structure, candidate writers, status values |
| `check-table-profile.js` | Profile section structure, classification, writer match |
| `check-procedure-catalog.js` | Statement entries, routing flags |
| `check-test-spec.js` | Branch manifest, unit test scenarios, coverage |
| `check-model-review.js` | Review checks, feedback items |
| `check-test-review.js` | Coverage scoring, quality issues |
| `check-command-summary.js` | Per-item statuses, total/ok/error counts |
| `check-status-output.js` | Stage statuses, recommendations |
| `check-refactored-sql.js` | Refactor section, extracted/refactored SQL |

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

Run once after the backend-specific `MigrationTest` fixture is materialized.
Re-run when the repo-managed fixture assets change.

```bash
docker ps | grep sql-test

export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=MigrationTest
export SA_PASSWORD=<your-password>

./scripts/sql/sql_server/materialize-migration-test.sh

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
