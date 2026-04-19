# Review Inputs

Use `$ARGUMENTS` as the target table FQN (`schema.table`).

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate context \
  --table <item_id> \
  [--writer <writer_fqn>]
```

Read generated artifacts from `dbt/models/`, `dbt/snapshots/`, and `dbt/seeds/` as needed.

## Artifact Lookup

- Check `dbt/models/marts/<model_name>.sql` with `dbt/models/marts/_marts__models.yml` for first-pass generated table/view targets.
- Check `dbt/snapshots/` for snapshot targets.
- Treat `dbt/models/staging/` as source-wrapper territory. If the selected migrated target is only present there, review it and flag naming/layer violations instead of returning `MODEL_NOT_FOUND`.
- Valid staging artifacts are pass-through `stg_bronze__<entity>.sql` wrappers plus `dbt/models/staging/_staging__sources.yml` and `dbt/models/staging/_staging__models.yml`.
- Return `MODEL_NOT_FOUND` when the selected artifact is missing either the SQL file or the paired schema YAML.

Derive `model_name` from the SQL filename and verify it matches [model-naming.md](../../_shared/references/model-naming.md).

## Project Inputs

- Read `dbt/dbt_project.yml` and verify layer defaults declare staging as `view`, intermediate as `ephemeral`, and marts as `table`.
- Read `test-specs/<item_id>.json` before the test-integration review.
- For every relation in `source_tables`, inspect the matching catalog table JSON first when available and note whether it is marked `is_source: true` or `is_seed: true`.
- If a relation is present in SQL but absent from `source_tables`, still inspect its matching catalog table JSON first when available before assigning dependency standards codes.
- If catalog metadata is missing or does not classify a possible seed, inspect generated dbt seed artifacts on disk before assigning dependency standards codes. A matching `dbt/seeds/<seed_name>.csv` plus `dbt/seeds/_seeds.yml` entry is sufficient to classify the relation as a seed dependency.

## Seed Inputs

To classify seed dependencies, prefer catalog metadata first. If catalog metadata is missing, inspect generated dbt seed artifacts on disk. If reviewed SQL references a seed with `ref('<seed_name>')`, verify:

- `dbt/seeds/<seed_name>.csv` exists
- `dbt/seeds/_seeds.yml` exists
- `_seeds.yml` includes the seed entry
- `_seeds.yml` includes known columns from catalog metadata

Seed dependencies referenced through direct `source()` or raw warehouse names are standards violations even when the SQL would compile.

Seed classification has precedence over generic source-wrapper classification: if catalog metadata marks the relation `is_seed: true`, or generated seed artifacts prove the relation is a seed, use `MDL_017` rather than `MDL_016`.

## Error Mapping

- `migrate context` exits 1 -> `status: "error"` with `CONTEXT_PREREQUISITE_MISSING`
- `migrate context` exits 2 -> `status: "error"` with `CONTEXT_IO_ERROR`
- generated model SQL or schema YAML missing -> `status: "error"` with `MODEL_NOT_FOUND`
- approved test spec missing -> `status: "error"` with `TEST_SPEC_NOT_FOUND`

Always return valid `ModelReviewResult` JSON, even on error paths.
