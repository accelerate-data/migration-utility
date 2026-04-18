# Artifact Writing

Use the migration CLIs as the write boundary. Do not write final SQL/YAML files directly.

## Build Artifacts

- SQL follows the shared dbt, SQL, CTE, naming, and artifact-invariant references.
- YAML follows `yaml-style` and describes the target artifact.
- Ordinary mart YAML merges into `models/marts/_marts__models.yml`.
- Snapshot YAML merges into `snapshots/_snapshots__models.yml`.
- Ordinary migrated targets must never be written under `models/staging/`. Staging is reserved for source wrappers created by setup-target.

Use `ref('stg_bronze__<entity>')` for confirmed source dependencies and
`ref('<seed_name>')` for seed dependencies. If a confirmed staging wrapper is
missing, stop with `GENERATION_FAILED`; do not fall back to direct
`source('bronze', ...)`.

## Render Unit Tests

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate render-unit-tests \
  --table <table_fqn> \
  --model-name <model_name> \
  --spec test-specs/<item_id>.json \
  --schema-yml .staging/schema.yml \
  --project-root <project_root>
```

The CLI owns canonical `unit_tests:` and maps confirmed source fixtures to `ref('stg_bronze__<entity>')`. Do not hand-write unit tests or mutate approved test specs. Report uncovered logic as warnings for `/generate-tests`.

## Write SQL and YAML

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate write \
  --table <table_fqn> \
  --model-sql-file .staging/model.sql \
  --schema-yml-file .staging/schema.yml \
  --project-root <project_root>
```

Use the CLI-returned paths in `artifact_paths`, including `snapshots/...` paths.
Do not hardcode mart or snapshot paths.

For ordinary migrated targets, inspect the returned paths before reporting success. If the SQL path is not `models/marts/<model_name>.sql` or the YAML path is not `models/marts/_marts__models.yml`, correct the write or return `GENERATION_FAILED`.

## Write Catalog Status

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate write-catalog \
  --table <table_fqn> \
  --model-path <relative_model_sql_path> \
  --compiled <true|false> \
  --tests-passed <true|false> \
  --test-count <number> \
  --schema-yml <true|false> \
  --project-root <project_root>
```

Pass `--warnings` and `--errors` as JSON arrays when needed.

Catalog status is:

- `ok` when the artifact exists and compile/build passed
- `partial` when the artifact exists but compile/build has warnings or failures
- `error` when the artifact is missing or unusable
