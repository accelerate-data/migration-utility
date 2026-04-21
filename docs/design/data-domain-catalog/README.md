# Data Domain Catalog

`classifying-data-domains` is a user-facing planning skill for breaking a whole-warehouse DDL snapshot into migration-ready business domains before `setup-source`, `/scope`, and mart migration workflows.

## Decisions

- Domain analysis runs in a whole-warehouse analysis repository, separate from one-domain migration repositories.
- Whole-warehouse DDL snapshots are required input and live under `warehouse-ddl/`.
- Whole-warehouse domain planning state lives under `warehouse-catalog/domains/`.
- Persist decisions, not object-inventory files. The current DDL/catalog remains the object inventory source.
- Reports, unresolved queues, and domain summaries are generated from the decision files and current DDL/catalog. They are not canonical state.
- Do not write compatibility domain files under legacy domain catalog paths.
- Keep `ddl/` and `catalog/` reserved for the existing one-domain migration pipeline.
- The skill should require an existing `warehouse-ddl/` directory and stop if it is missing.
- The skill should write decision files only when the user explicitly asks it to persist the analysis.

## Decision Files

Persist exactly these decision files:

```text
warehouse-catalog/domains/
  sales.json
  finance.json
  operations.json
```

Each domain file records the canonical accepted state for one business domain:

```json
{
  "schema_version": 1,
  "domain": "Sales",
  "slug": "sales",
  "status": "candidate",
  "description": "Revenue and order lifecycle tables.",
  "confidence": "medium",
  "objects": {
    "tables": ["gold.fact_sales"],
    "views": []
  },
  "setup_source_candidates": {
    "schemas": ["gold"],
    "tables": ["gold.fact_sales"]
  },
  "dependencies": {
    "upstream_domains": [],
    "downstream_domains": []
  },
  "ambiguities": [],
  "rationale": [
    "Name and direct joins indicate the sales order lifecycle."
  ]
}
```

Unresolved ownership appears in `ambiguities` and blocks persistence until the user chooses a primary domain.

## Layer Decisions

Layer decisions cover every table or view in the whole-warehouse inventory.

Supported layer decisions:

- `source`
- `staging`
- `ods`
- `warehouse`
- `etl_control`
- `unknown`

Source, staging, and ETL-control objects do not receive domain decisions. ODS objects receive domain decisions. Warehouse objects receive both domain decisions and table-classification decisions.

Layer detection uses both database/schema names and object prefixes. Database/schema evidence takes precedence when signals conflict.

## Domain Decisions

Domain decisions apply only to ODS and warehouse objects. Every ODS or warehouse object must belong to exactly one domain unless its domain decision is unresolved.

Domain grouping starts from fact tables and fact variants. Facts define business processes and seed candidate domains. Directly joined dimensions are included when ownership is clear.

Conformed dimensions use subject-domain ownership when the subject is clear. For example, `dim_customer` belongs to Customer even when it is used by Sales, Services, and Finance facts.

Same-grain derived facts remain in the base fact's domain unless the derived fact adds domain-specific business semantics or enrichment. If ownership is unclear, leave the domain decision null.

Aggregates inherit domain from the object named in `aggregate_of`. Bridge tables and minidimensions inherit domain from the object named in `supports_dimension`.

Junk dimensions may be classified from DDL, but their domain decision should usually remain null because DDL does not show value-level ownership.

## Table Classification Decisions

Table classification applies only to warehouse objects.

Fact classification types:

- `fact`
- `transaction_fact`
- `periodic_snapshot_fact`
- `accumulating_snapshot_fact`
- `factless_fact`

Dimension classification types:

- `dimension`
- `scd`
- `degenerate_dimension`
- `junk_dimension`
- `role_playing_dimension`
- `date_dimension`

Dimension support classification types:

- `bridge_table`
- `minidimension`

Dimension support decisions must include `supports_dimension`.

Aggregate classification types:

- `aggregate_fact`
- `aggregate_dimension`

Aggregate decisions must include `aggregate_of`.

Reference or lookup naming is evidence for the base `dimension` type. It is not a separate table classification type.

Unresolved table classification does not block domain placement. Later mart migration commands may resolve classification within an already-decided domain.

## Date Dimension Decision

Date dimensions are a global decision, not ordinary domain-owned objects.

`date-dimension-decision.json` records all detected date/calendar candidates, the canonical decision, and the reason. The classifier should disambiguate multiple calendar-like tables and select one canonical date dimension when the evidence supports it.

Generated domain outputs should include the canonical date dimension as a required shared dimension for each domain. Date dimensions do not count toward domain-size heuristics and must not drive domain ownership.

## Domain Size Heuristic

Domain size warnings count only core facts and core dimensions.

Exclude these objects from the size heuristic:

- aggregates
- minidimensions
- bridge tables
- date dimensions

Warning bands:

- fewer than 10 core objects: too narrow
- 20 to 50 core objects: good
- 50 to 70 core objects: probably too wide
- 100 or more core objects: break up the domain

These bands guide review. They do not replace ownership evidence.

## Generated Reports

Generated reports may be printed in the response. They are derived from domain files and current DDL evidence; they are not canonical state.

Reports should include:

- layer counts by schema and decision
- domain sizes and domain dependencies
- unresolved layer and domain decisions
- classification-pending objects by domain
- date-dimension candidates and selected canonical date dimension

Reports and unresolved queues are derived from decision files. They are not canonical state.

## Repository Modes

Whole-warehouse analysis repositories use:

```text
warehouse-ddl/
warehouse-catalog/domains/
```

Domain migration repositories use:

```text
ddl/
catalog/tables/
catalog/views/
catalog/procedures/
catalog/functions/
```

`classifying-data-domains` consumes only `warehouse-ddl/` and writes only under `warehouse-catalog/domains/` when persistence is requested.

The one-domain migration pipeline consumes `ddl/` and `catalog/`. It must not write domain decomposition state.

## Input Guard

`warehouse-ddl/` is mandatory. `classifying-data-domains` must check for it before analysis.

If `warehouse-ddl/` is missing:

- stop immediately
- do not create `warehouse-ddl/`
- do not accept pasted DDL, ad hoc table lists, or ERD text as a substitute
- tell the user to run the warehouse DDL extraction workflow first

DDL extraction and warehouse-DDL folder creation belong to a separate workflow.

## Idempotency And Reruns

Reruns preserve user decisions. LLM decisions may be recomputed when candidates or evidence change materially.

Rules:

- the same accepted state serializes to the same JSON
- decision maps are sorted by object FQN before writing
- volatile timestamps are not written
- unresolved layer or domain decisions are represented with `decision: null`
- source, staging, and ETL-control objects never receive domain decisions
- ODS and warehouse objects have exactly one domain decision when resolved
- unresolved ODS or warehouse domain decisions are excluded from generated canonical domain membership
- generated domain summaries are rebuilt from decision files and current DDL/catalog

When the user changes an object's layer, domain, or table-classification decision, rerun affected downstream evaluation so inherited domain assignments and generated reports stay consistent.

## Skill Boundary

`classifying-data-domains` may explain or persist classification decisions when requested, but it does not run extraction, mutate setup-source configuration, or start migration commands.

Future setup-source integration should read `warehouse-catalog/domains/` decision files and the corresponding `warehouse-ddl/` snapshot as user-approved planning input, then create the selected domain migration repo state under `ddl/` and `catalog/`.
