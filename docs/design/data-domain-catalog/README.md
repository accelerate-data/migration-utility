# Data Domain Catalog

`classifying-data-domains` is a user-facing planning skill for breaking a warehouse into
migration-ready business domains before `setup-source`, `/scope`, and mart migration
workflows.

## Decisions

- Domain analysis runs in a whole-warehouse analysis repository, separate from
  one-domain migration repositories.
- Whole-warehouse DDL snapshots are required input and live under `warehouse-ddl/`.
- Whole-warehouse domain planning state lives under `warehouse-catalog/`.
- Persist data-domain planning as one canonical JSON file per domain under
  `warehouse-catalog/data-domains/<slug>.json`.
- Do not create an `index.json`; consumers should discover domains by scanning
  `warehouse-catalog/data-domains/*.json`.
- Keep `ddl/` and `catalog/` reserved for the existing one-domain migration
  pipeline.
- Treat each domain file as current accepted state, not as generated output plus
  manual override layers.
- The skill should require an existing `warehouse-ddl/` directory and stop if it is
  missing.
- The skill should produce a human-readable report and one machine-readable JSON
  object per domain.
- The skill should write domain files only when the user explicitly asks it to
  persist the analysis.
- `setup-source` ingestion from domain files is a follow-on CLI capability, not part
  of the skill contract cleanup.

## Domain File Contract

Each domain file owns one primary business domain and the warehouse objects assigned
to it.

Required stable fields:

- `schema_version`
- `domain`
- `slug`
- `status`
- `description`
- `confidence`
- `objects`
- `setup_source_candidates`
- `dependencies`
- `ambiguities`
- `rationale`

The `objects` section groups primary table and view members by catalog object
type. Procedures and functions are not domain-catalog objects.

```json
{
  "objects": {
    "tables": ["silver.fact_sales"],
    "views": ["gold.vw_sales_summary"]
  }
}
```

`setup_source_candidates` identifies the source schemas and objects the user should
consider when moving from domain analysis into source setup.

## Repository Modes

Whole-warehouse analysis repositories use:

```text
warehouse-ddl/
warehouse-catalog/data-domains/
```

Domain migration repositories use:

```text
ddl/
catalog/tables/
catalog/views/
catalog/procedures/
catalog/functions/
```

`classifying-data-domains` consumes only `warehouse-ddl/` and writes only to
`warehouse-catalog/` when persistence is requested.

The one-domain migration pipeline consumes `ddl/` and `catalog/`. It must not write
domain decomposition state.

## Input Guard

`warehouse-ddl/` is mandatory. `classifying-data-domains` must check for it before
analysis.

If `warehouse-ddl/` is missing:

- stop immediately
- do not create `warehouse-ddl/`
- do not create `warehouse-catalog/`
- do not accept pasted DDL, ad hoc table lists, or ERD text as a substitute
- tell the user to run the warehouse DDL extraction workflow first

The skill may read DDL files already present under `warehouse-ddl/`, but DDL
extraction and warehouse-DDL folder creation belong to a separate workflow.

## Idempotency

Reruns rewrite canonical state.

Rules:

- the same domain name maps to the same slug and file path
- the same accepted state serializes to the same JSON
- arrays are sorted before writing
- field order is stable
- volatile timestamps are not written
- only impacted domain files are rewritten
- each table or view has exactly one primary domain
- duplicate primary assignments are conflicts that require user resolution

When the user changes an object's domain assignment, the agent rewrites the impacted
canonical files directly. It should not preserve separate manual include or exclude
lists.

## Dependency Semantics

If object A references object B, A depends on B. B must be available before A for
load planning.

For domain ordering, a domain's upstream domains are domains that must be available
before this domain can be migrated. Downstream domains depend on this domain.

## Skill Boundary

`classifying-data-domains` may explain or persist domain files when requested, but it
does not run extraction, mutate setup-source configuration, or start migration
commands.

Future setup-source integration should read
`warehouse-catalog/data-domains/<slug>.json` files and the corresponding
`warehouse-ddl/` snapshot as user-approved planning input, then create the selected
domain migration repo state under `ddl/` and `catalog/`.
