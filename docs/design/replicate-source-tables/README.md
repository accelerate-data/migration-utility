# Replicate Source Tables

## Decision

Add `ad-migration replicate-source-tables` as a deterministic CLI that copies data from `runtime.source` into the target-side source tables configured by `runtime.target`.

## Boundary

The command prepares target input data only. It does not run dbt, review generated models, write model-generation status, or compare model output.

Users and agents run normal dbt commands after replication, for example `dbt build --select +<model>`.

## Source and Target

The source endpoint is always `runtime.source`.

The destination endpoint is always `runtime.target`, using the target source schema and table mapping produced by `setup-target`.

The CLI must not expose alternate runtime or target-schema options in the initial contract.

## Copy Semantics

Replication always uses truncate-load semantics for the selected target source tables.

Multi-table runs continue after per-table failures, then return a non-zero exit code if any selected table failed.

## Required Row Cap

`--limit` is mandatory and bounded to `1..10000`.

Omitting it fails with `LIMIT_REQUIRED`; exceeding the cap fails with `LIMIT_TOO_HIGH`.

The row cap is a safety boundary, not a sampling guarantee.

## Developer Options

The initial CLI surface should stay small:

- `--limit <n>`: required row cap.
- `--select <fqn>`: include only specific confirmed source tables.
- `--exclude <fqn>`: omit specific confirmed source tables from the default set.
- `--filter <fqn=predicate>`: append a raw source-side SQL predicate to the selected table query.
- `--dry-run`: print the copy plan without truncating or copying data.
- `--json`: emit structured output.
- `--yes`: skip destructive confirmation.

`--filter` is bound to a table by splitting on the first `=`. The predicate itself is not parsed; it is appended to `select * from <source_table> where <predicate>`.

## Out of Scope

Do not include alternate modes such as append, replace-schema, source-runtime selection, target-schema override, or dbt execution in the initial command.

Those choices make the command harder to reason about and belong in later designs if real operator needs appear.
