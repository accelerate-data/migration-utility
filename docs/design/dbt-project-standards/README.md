# dbt Project Standards

Generated dbt projects use dbt layer folders as the organizing structure: `models/staging/`, `models/intermediate/`, and `models/marts/`. The migration source schemas do not define folder names.

This decision follows dbt's project-structure guidance for staging, intermediate, marts, and directory-scoped YAML:

- Staging guidance: https://docs.getdbt.com/best-practices/how-we-structure/2-staging?version=1.12
- Intermediate guidance: https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate?version=1.12
- Marts guidance: https://docs.getdbt.com/best-practices/how-we-structure/4-marts?version=1.12
- YAML and project defaults guidance: https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project?version=1.12
- Model style guidance: https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models?version=1.12
- SQL style guidance: https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql?version=1.12
- YAML style guidance: https://docs.getdbt.com/best-practices/how-we-style/5-how-we-style-our-yaml?version=1.12

## Layer Layout

`setup-target` scaffolds this project shape:

```text
dbt/models/
  staging/
    _staging__sources.yml
    _staging__models.yml
    stg_bronze__<entity>.sql
  intermediate/
    _intermediate__models.yml
    int_<entity>_<purpose>.sql
  marts/
    _marts__models.yml
    <entity>.sql
```

`models/staging/` contains both dbt `source()` declarations and 1:1 staging wrapper models. `models/intermediate/` is reserved for reusable transformation steps introduced by later refactor workflows. First-pass migrated target models are written under `models/marts/`.

## Source Namespace

Use `bronze` as the canonical source namespace in dbt source declarations and staging model names. The tool must not infer source systems from warehouse schemas or table names.

Staging wrapper names follow dbt's `stg_[source]__[entity]` pattern. Because generated migration projects use `bronze` as the source namespace, the concrete generated name is `stg_bronze__<entity>`.

Examples:

```text
source('bronze', 'Customer')
stg_bronze__customer
```

Physical source schema mapping remains a runtime/setup concern. The dbt source name stays `bronze`.

## Initial Staging Models

Initial staging models are pure pass-through wrappers over confirmed source tables.

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

The catalog does not prove safe transformation intent, so setup must not invent staging cleanup logic. Later mart-refactor work may move proven row-preserving transformations into staging when there is evidence across generated marts.

## Generated Marts

First-pass generated migrated target models are mart models, not staging models. They should be written to `models/marts/<model_name>.sql` with their metadata in `models/marts/_marts__models.yml`.

Generated mart SQL should prefer existing staging wrappers over direct source references. A mart that needs a confirmed bronze table should normally read `ref('stg_bronze__<entity>')`, not `source('bronze', '<table>')`.

The model filename may differ from the physical warehouse target if preservation requires dbt config. Preserve physical target names or schemas through dbt model configuration when required by migration fidelity; do not encode legacy warehouse schemas as folders.

First-pass generated mart model names use the normalized target entity name without layer prefixes. Future intermediate models use `int_<entity>_<purpose>` when a later workflow introduces them.

## SQL Style

Generated and refactored dbt-bound SQL adopts dbt Labs SQL style. The shared SQL rules must match dbt Labs guidance, including trailing commas and positional grouping such as `group by 1, 2`.

`refactoring-sql` consumes only the SQL and CTE style subset. It should produce dbt-style CTE shape for downstream model generation, but it does not own dbt paths, materializations, source wrappers, or model names.

## YAML Style

Generated dbt YAML adopts dbt Labs YAML style. The shared YAML rules must match dbt Labs guidance, including 2-space indentation, indented list items, readable blank lines between dictionary list items where helpful, and explicit argument structures where dbt supports them.

## dbt Project Evaluator

Use dbt Project Evaluator as a refactor-time quality gate for workflows that change the dbt project graph, especially `refactor-mart` candidate execution. Run it after the changed models build successfully.

Evaluator findings are review evidence, not automatic rewrite instructions. Fix findings that align with dbt Labs guidance and this generated-project standard. Document exceptions when a rule conflicts with migration fidelity, generated-project constraints, or an explicit user decision.

Initial `setup-target` scaffolding does not depend on dbt Project Evaluator.

## Project Defaults

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

## Skill Guidance

`generating-model` must consume the plugin-local shared dbt standards reference plus the shared model, SQL, and YAML references. It writes first-pass migrated target models into `models/marts/` and should reference existing staging wrappers for confirmed bronze sources.

`reviewing-model` must consume the same plugin-local shared dbt standards reference plus the shared model, SQL, and YAML references. It reviews generated target models under `models/marts/`, reviews staging wrappers under `models/staging/`, and flags direct `source()` usage in mart models when the matching `stg_bronze__*` wrapper exists.

`refactoring-sql` must consume the shared SQL and CTE style references only. It preserves source semantics and source dialect syntax while shaping SQL for clean downstream dbt generation.
