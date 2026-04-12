# Stage 3 -- dbt Scaffolding

`/init-dbt` scaffolds the dbt project for the selected target platform and generates `sources.yml` from the catalog.

## Prerequisites

- `manifest.json` must exist
- `catalog/tables/` must exist
- the initial analyze stage must be complete for in-scope tables

Before `/init-dbt` can proceed, extracted tables need to be in one of these states:

- resolved to a writer
- excluded from the migration
- writerless and explicitly confirmed as a source

## Target selection

The command prompts for the target adapter. Current options include Fabric Lakehouse, Spark, Snowflake, SQL Server, and DuckDB.

## What it writes

```text
dbt/
  dbt_project.yml
  profiles.yml
  packages.yml
  models/
    staging/
      sources.yml
    marts/
  macros/
  seeds/
  tests/
```

## `sources.yml` behavior

This is the part that matters most operationally:

- tables with `is_source: true` are included in `sources.yml`
- writerless tables with `scoping.status == "no_writer_found"` but no `is_source` flag are left in the unconfirmed bucket
- resolved migration targets are excluded from `sources.yml` because they are expected to become dbt models
- excluded tables do not appear in `sources.yml`

So `no_writer_found` by itself is not enough. Source tables have to be explicitly confirmed, either with `/add-source-tables` or during the `/init-dbt` confirmation flow.

## Re-running

Re-running `/init-dbt` is safe:

- it regenerates `sources.yml`
- it does not overwrite your edited `profiles.yml`
- it does not overwrite generated models or snapshots

## Next step

Proceed to [[Stage 4 Sandbox Setup]].
