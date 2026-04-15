# Stage 3 -- dbt Scaffolding

`ad-migration setup-target` scaffolds the dbt project and generates `sources.yml` from the catalog.

## Invocation

```bash
ad-migration setup-target --technology fabric
```

| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `fabric`, `snowflake`, or `duckdb` |
| `--source-schema` | no | Source schema for `sources.yml` (defaults to `bronze`) |

## Prerequisites

- `manifest.json` must exist
- `catalog/tables/` must exist
- the initial analyze stage must be complete for in-scope tables

Before `/setup-target` can proceed, extracted tables need to be in one of these states:

- resolved to a writer
- excluded from the migration
- writerless and explicitly confirmed as a source

## Target selection

The command prompts for the target adapter. Current options include SQL Server and Oracle.

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

So `no_writer_found` by itself is not enough. Source tables have to be explicitly confirmed, either with `/add-source-tables` or during the `/setup-target` confirmation flow.

## Re-running

Re-running `/setup-target` is safe:

- it regenerates `sources.yml`
- it does not overwrite your edited `profiles.yml`
- it does not overwrite generated models or snapshots

## Next step

Proceed to [[Stage 4 Sandbox Setup]].
