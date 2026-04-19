# dbt Project Standards

Use these rules for generated dbt artifacts.

## Project Layout

Generated dbt projects use these model layer folders:

```text
models/
  staging/
  intermediate/
  marts/
```

Do not create source-system, warehouse-schema, or guessed business-domain subfolders during first-pass migration.

Layer YAML files live next to the artifacts they describe:

```text
models/staging/_staging__sources.yml
models/staging/_staging__models.yml
models/intermediate/_intermediate__models.yml
models/marts/_marts__models.yml
snapshots/_snapshots__models.yml
```

## Source Namespace

Use `bronze` as the generated project source namespace.

Staging wrappers follow dbt's `stg_[source]__[entity]` pattern. Because the source namespace is `bronze`, generated staging wrapper names are:

```text
stg_bronze__<entity>
```

Do not infer source-system names from source schemas, target schemas, table names, or procedure names.

## Initial Staging Wrappers

Initial staging wrappers are pure 1:1 pass-through models over confirmed sources.

They may:

- reference exactly one `source('bronze', '<table>')`
- preserve source column names exactly
- preserve source grain exactly

They must not:

- cast
- rename
- categorize
- compute new business columns
- filter
- join
- aggregate
- change grain

Use this shape:

```sql
with

source as (

    select * from {{ source('bronze', '<table>') }}

),

final as (

    select * from source

)

select * from final
```

## Generated Marts

First-pass generated migrated target models are mart models, not staging models.

Write them under:

```text
models/marts/<model_name>.sql
models/marts/_marts__models.yml
```

Mart model names use the normalized target entity name without layer prefixes.

Generated mart SQL uses staging wrappers for confirmed source dependencies. For a confirmed source table, use:

```sql
{{ ref('stg_bronze__<entity>') }}
```

instead of direct source references:

```sql
{{ source('bronze', '<table>') }}
```

If the wrapper is missing for a confirmed source dependency, treat that as a setup/artifact problem. Do not silently fall back to direct `source()` in generated mart SQL.

Preserve physical target names or schemas through dbt model config when migration fidelity requires it. Do not encode legacy warehouse schemas as folders.

Reviewer code: use `MDL_016` when mart SQL bypasses a confirmed staging wrapper.

## Snapshots

Snapshot targets are written under `snapshots/` by `migrate write`.
Snapshot YAML uses a top-level `snapshots:` key in `snapshots/_snapshots__models.yml`.

## Intermediate Models

First-pass model generation does not create intermediate models.

Later refactor workflows may create intermediate models when shared transformation logic is proven. Intermediate model names use:

```text
int_<entity>_<purpose>
```

## Layer Defaults

`dbt_project.yml` owns default materialization by layer:

```yaml
models:
  <project_name>:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
```

Use model-level config only for exceptions such as migration-required aliases, schemas, incremental models, or snapshots.

Do not set redundant `materialized='table'` config on ordinary first-pass mart table models; the `marts` layer default already supplies it. View models, incremental models, aliases, and schema-preservation cases are explicit exceptions.

## Seeds

`setup-target` writes confirmed seed CSVs under:

```text
seeds/<seed_name>.csv
seeds/_seeds.yml
```

Seed CSVs must remain in the dbt seed path, not under `models/`. Document seeds in YAML under a top-level `seeds:` key and include known columns. Include `data_type` when catalog/source metadata provides it.

Downstream models reference seeds with `{{ ref('<seed_name>') }}`.
If a migrated model joins to a catalog table marked `is_seed: true`, treat that table as an existing dbt seed dependency and use `ref()`. Do not model seed dependencies as `source()` relations or raw warehouse table names.

Reviewer code: use `MDL_017` when mart SQL bypasses a seed `ref()`.

## Skill Boundaries

`generating-model` uses these full project, naming, SQL, and YAML standards when writing dbt artifacts.

`reviewing-model` enforces these full project, naming, SQL, and YAML standards when reviewing dbt artifacts.

`refactoring-sql` uses only the shared SQL and CTE style references. It must not make dbt folder, model-name, source-wrapper, or materialization decisions.

YAML generated for sources, models, and tests must follow the shared YAML style reference.

## dbt Project Evaluator

Use dbt Project Evaluator as a refactor-time quality gate for workflows that change the dbt project graph, especially `refactor-mart` candidate execution.

Run it after the changed models build successfully. Treat findings as review evidence, not automatic rewrite instructions.

Rules that conflict with migration fidelity or this generated-project standard are not fixed blindly. Document the exception or follow-up instead.

Evaluator use belongs in refactor validation. Initial `setup-target` scaffolding should not depend on running dbt Project Evaluator.
