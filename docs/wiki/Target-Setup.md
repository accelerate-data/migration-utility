# Target Setup

`ad-migration setup-target` prepares the target dbt project and validates the source-facing dbt layer.

Run it after DDL extraction, scoping, and source or exclude decisions. The command consumes catalog decisions; it is not where you decide whether a table is a source, seed, migration target, or excluded object.

## Invocation

```bash
ad-migration setup-target
ad-migration setup-target --source-schema bronze
```

## What it does

`setup-target`:

- writes target runtime state into `manifest.json`
- scaffolds the `dbt/` project
- writes the dbt profile used by downstream validation
- generates staging source metadata from confirmed source tables
- generates pass-through `stg_bronze__<entity>` wrappers for confirmed source tables
- exports confirmed seed tables as dbt seed CSVs
- creates any missing target-side source tables
- validates the generated staging/source setup artifacts

## Source and seed decisions

Before running target setup, extracted tables should have one of these outcomes:

- scoped to a writer and still in the migration pipeline
- confirmed as a source with `ad-migration add-source-table <fqn>`
- confirmed as a seed with `ad-migration add-seed-table <fqn>`
- excluded with `ad-migration exclude-table <fqn>`

Only confirmed source tables are included in staging source metadata. Writerless tables are not automatically treated as sources.

## dbt validation during setup

Target setup validates only the artifacts it generated for the source-facing layer:

- runs `dbt seed` when seed CSVs were exported
- runs `dbt compile` for generated staging wrappers
- runs `dbt build` for generated staging wrappers and source selectors
- excludes dbt unit tests from that build

This validation proves the generated source metadata, staging wrappers, seed files, target profile, and target connection are coherent.

## How downstream commands use it

Downstream commands rely on target setup in different ways:

| Command | Uses target setup for |
|---|---|
| `/migrate-mart-plan` | target and dbt readiness gates |
| `/generate-tests` | source wrappers and target-side dbt project state |
| `/generate-model` | `runtime.target`, dbt project files, dbt profile, and staging wrappers |
| `/refactor-mart` | dbt project files and candidate-scoped validation |

`/generate-model` does not run a broad `dbt build` for each generated mart model. It compiles the generated model, materializes direct parents with an empty run when unit tests need them, then runs scoped dbt unit tests.

`/refactor-mart` uses candidate-scoped validation from the approved refactor-mart plan. Those validation commands commonly use `dbt build --select <model-or-scope>`.

## Rerunning

Rerun `ad-migration setup-target` after changing source or seed decisions. It refreshes staging source metadata and seed files, then creates only missing target-side source tables.

Target setup is guarded once downstream generated dbt models exist. If you need to change source or seed decisions after model generation, use `/status` to identify the affected objects and reset or regenerate the downstream state deliberately.

## Related pages

- [[Status Dashboard]]
- [[dbt Scaffolding]]
- [[Model Generation]]
- [[Sandbox Operations]]
