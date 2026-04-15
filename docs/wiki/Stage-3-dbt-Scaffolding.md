# Stage 3 -- dbt Scaffolding

`ad-migration setup-target` scaffolds the dbt project and generates `sources.yml` from the catalog.

## Invocation

```bash
ad-migration setup-target
```

| Option | Required | Description |
|---|---|---|
| `--source-schema` | no | Source schema for `sources.yml` (defaults to `bronze`) |
| `--project-root` | no | Defaults to current working directory |

`setup-target` reads the target technology from `manifest.json` as `runtime.target`, which is seeded by `/init-ad-migration`.

## Prerequisites

- `manifest.json` must exist
- `catalog/tables/` must exist
- the initial analyze stage must be complete for in-scope tables

Before `ad-migration setup-target` can proceed, extracted tables need to be in one of these states:

- resolved to a writer
- excluded from the migration
- writerless and explicitly confirmed as a source

In practice, this means you should finish the scope/exclude/source decision first, then run `ad-migration add-source-table <fqn>` for every table that should remain a dbt source before invoking `ad-migration setup-target`.

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

So `no_writer_found` by itself is not enough. Source tables have to be explicitly confirmed with `ad-migration add-source-table <fqn>` before `ad-migration setup-target`. `setup-target` consumes those decisions; it should not be the step where you make them.

If you confirm additional source tables later, rerun `ad-migration setup-target`. The command is idempotent: it will regenerate `sources.yml` from the latest `is_source` flags and create only the missing target-side source tables.

## Re-running

Re-running `ad-migration setup-target` is safe:

- it regenerates `sources.yml`
- it does not overwrite your edited `profiles.yml`
- it does not overwrite generated models or snapshots
- it creates any missing target-side source tables for items already marked `is_source: true`, but it does not backfill data or decide which tables are sources

## Next step

Proceed to [[Stage 4 Sandbox Setup]].
