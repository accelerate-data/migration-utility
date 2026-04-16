# Seed Table Catalog State

Writerless tables have two valid ownership modes.

- Source tables are owned by ingestion or extract-load processes and are declared as dbt sources.
- Seed tables are owned by dbt as version-controlled seed data and are loaded by `dbt seed`.

The migration catalog must represent those modes explicitly because downstream agents need to
distinguish ingestion-owned inputs from dbt-owned reference data before applying profiling,
test-generation, refactoring, or model-generation assumptions.

## Decision

Represent seed tables with a table-level `is_seed` flag. Keep `is_source` as the existing
ingestion-source marker.

`is_source` and `is_seed` are mutually exclusive:

- `is_source: true` means the table is an ingestion-owned source table.
- `is_seed: true` means the table is a dbt-owned seed table.
- setting one flag to `true` must clear the other flag.

Writerless tables continue to default to source-table treatment unless the user explicitly marks
them as seed tables.

## CLI Ownership

Seed classification is a deterministic catalog mutation, not an LLM profiling inference.

`ad-migration add-seed-table <fqn>` marks an existing catalog table as a seed table and pulls it out
of the migration workflow. If the table was already marked as a source table, the command flips it
from `is_source: true` to `is_seed: true`.

`ad-migration add-source-table <fqn>` remains the command for ingestion-owned source tables. If the
table was already marked as a seed table, the command flips it from `is_seed: true` to
`is_source: true`.

Both commands require an existing analyzed table catalog entry. They do not create dbt seed CSV
files or source YAML.

## Target Setup

`ad-migration setup-target` exports confirmed seed tables from the configured source database into
`dbt/seeds/<table>.csv`, then runs `dbt seed` with the generated project and profile. Seed tables
materialize in the configured target source schema, which defaults to `bronze`.

## Profiling Semantics

Profiling a seed table must persist a profile that records seed semantics instead of applying the
regular writer-driven table questions. The persisted profile must let status and batch reports show
that the table is intentionally seed-backed.

Regular source tables keep existing source-table handling. Non-seed migration tables keep the
existing table profiling flow.

## Status And Batch Reporting

Status and readiness output must report seed tables distinctly from source tables. Seed tables are
not active migration candidates, but they are not ingestion sources either.

Batch planning must expose seed tables with their own count and list. They must not be mixed into
source-table, writerless, or profile-needed buckets.

Catalog browsing must expose seed tables through a dedicated seed listing path. A request to list
ordinary tables must not treat seed tables as active migration tables, and a request to list sources
must remain limited to ingestion-owned source tables.

## dbt Rationale

The distinction follows dbt ownership boundaries. Sources describe warehouse tables loaded by
extract-load tools; seeds describe small, static, version-controlled CSV data maintained inside the
dbt project.

Seed status therefore belongs in catalog state as an explicit user decision. Inferring seed status
from the absence of a writer is too broad, and inferring it only from dbt seed files is too late for
planning because the seed CSV may not exist yet.
