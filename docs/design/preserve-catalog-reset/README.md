# Preserve-Catalog Reset

## Decision

`ad-migration reset all --preserve-catalog` clears generated target/dbt state while preserving extracted catalog files, DDL files, and source analysis work.

The command deletes the whole `dbt/` directory. The user must run `ad-migration setup-target` again to recreate `dbt_project.yml`, `profiles.yml`, source/staging models, seed files, and dbt-generated directories.

## Reason

Full reset removes `catalog/` and `ddl/`, forcing users to rerun setup-source, scoping, and profiling. Per-object reset blocks after model generation. Preserve-catalog reset gives operators a clean target/dbt regeneration path without repeating source extraction and source analysis.

## Deleted Paths

The command deletes these project paths when present:

- `dbt/`
- `test-specs/`
- `.staging/`
- `.migration-runs/`

It does not delete `catalog/` or `ddl/`.

## Preserved Manifest State

The command preserves the manifest state needed to avoid rerunning setup-source and to rerun setup-target:

- source runtime configuration
- target technology selection
- extraction metadata
- init handoff metadata

Setup-target still rewrites target connection details from the current environment and recreates the deleted `dbt/` directory.

## Cleared Catalog Sections

The command mutates existing catalog files in place and reports every section it clears.

For every table catalog in `catalog/tables/`, clear:

- `test_gen`
- `generate`
- `refactor`, if present

For every view or materialized-view catalog in `catalog/views/`, clear:

- `test_gen`
- `refactor`
- `generate`

For every procedure catalog in `catalog/procedures/`, clear:

- `refactor`

Function catalogs are preserved unchanged.

## Preserved Catalog Sections

The command preserves extraction, scoping, and profiling facts:

- table/view identity, columns, keys, constraints, sensitivity, references, and source diagnostics
- table/view `scoping`
- table/view `profile`
- table flags such as `is_source`, `is_seed`, `excluded`, `ddl_hash`, and `stale`
- procedure references, statements, routing metadata, `table_slices`, DDL diagnostics, and dependency metadata
- catalog diagnostic review artifacts

This preserves the work needed to continue from setup-target rather than setup-source.

## Safety Boundary

The command does not try to merge, preserve, or selectively repair generated dbt files. Deleting `dbt/` is intentional because setup-target owns the full bootstrap of the target dbt project.

If any catalog file cannot be read or rewritten safely, the command fails before reporting a successful reset.

## Resume Point

After a preserve-catalog reset, the next command is `ad-migration setup-target`. The pipeline then resumes from preserved scope/profile state and regenerated setup-target artifacts.

## Dependency Boundary

This reset mode is independent of target type mapping. It must exist before setup-target rerun behavior points users at a preserve-catalog recovery path.
