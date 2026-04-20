---
name: analyze-data-domains
description: >
  Use when the user wants to break a whole-warehouse DDL snapshot into business data domains for migration planning, setup-source selection, or domain-scoped scope and migrate-mart workflows.
user-invocable: true
argument-hint: "<warehouse-ddl/> [persist only when explicitly requested]"
---

# Analyze Data Domains

Break a whole-warehouse DDL snapshot into migration-ready business domains. This
skill is a planning step before `setup-source`, `/scope`, and mart migration
workflows.

## Scope

Use this skill in a warehouse-analysis repository, not a one-domain migration
repository.

Required input:

- `warehouse-ddl/` with the whole-warehouse DDL snapshot

Outputs:

- a human-readable domain analysis report
- one machine-readable JSON object per domain in the response
- optional persisted files under `warehouse-catalog/data-domains/<slug>.json`,
  only when the user explicitly asks to persist the analysis

This skill does not:

- create or populate `warehouse-ddl/`
- run DDL extraction
- read `ddl/` as input
- write `catalog/`
- run `setup-source`, `/scope`, `/profile`, or migration commands
- accept pasted DDL, ad hoc table lists, or ERD text as substitutes for
  `warehouse-ddl/`

## Input Guard

`warehouse-ddl/` is required. Check for it before any analysis.

If `warehouse-ddl/` is missing:

- stop immediately
- Do not create `warehouse-ddl/`
- Do not create `warehouse-catalog/`
- Do not accept pasted DDL, ad hoc table lists, or ERD text as a substitute
- tell the user to run the warehouse DDL extraction workflow first

Proceed only when `warehouse-ddl/` exists.

## When to Ask

Ask for more input only when a required human decision is missing, such as:

- which ambiguous primary domain should own an object
- whether to persist the analysis after presenting the report
- how to resolve the same object appearing in multiple primary domains

Proceed with low confidence when `warehouse-ddl/` exists but evidence is weak.
Mark weak classifications with `confidence: "low"` and explain the missing
evidence instead of inventing definitions.

## Domain Analysis Flow

1. Inventory DDL files under `warehouse-ddl/`.
2. Extract objects from available DDL:
   - tables
   - views
   - procedures
   - functions
3. Classify each object by dimensional modeling role.
4. Assign exactly one primary business domain per object.
5. Map object and domain dependencies.
6. Identify ambiguities and conflicts.
7. Present a report plus one JSON object per domain.
8. Persist domain files only if the user explicitly requested persistence.

Use `references/22_dw_table_patterns.md` for dimensional role classification.
Use `references/21_domain_taxonomy.md` for business-domain assignment.

## Role Classification

Assign each table one role. Apply strong naming and structural evidence before
weaker inference.

| Role | Strong Signals |
|---|---|
| Staging | `STG_`, `STAGE_`, `RAW_`, `LAND_`, `SRC_`; raw source columns |
| Fact | `FACT_`, `FCT_`; multiple foreign keys plus additive measures |
| Aggregate | `AGG_`, `SUMM_`, `RPT_`, `ROLLUP_`; coarser-grain fact summary |
| Dimension | `DIM_`; descriptive attributes; surrogate key; SCD columns |
| Bridge | `BRG_`, `BRIDGE_`, `XREF_`; many-to-many relationship structure |
| Reference | `LKP_`, `REF_`, `LOOKUP_`, `CODE_`; compact code-description table |
| ODS | `ODS_`, `CURR_`, `CURRENT_`; source-like current-state structure |
| Unknown | insufficient evidence |

For each object, record role, confidence, and evidence.

## Domain Assignment

Assign each object to exactly one primary business domain. Secondary domain tags
are allowed only as descriptive metadata; they do not change primary ownership.

Use this evidence order:

1. explicit user-provided ownership decisions
2. table, view, or procedure name signals
3. column-name signals
4. dependency graph position
5. industry-specific terms from `references/21_domain_taxonomy.md`
6. `Unclassified` with low confidence

If a user moves an object between domains, rewrite the impacted canonical domain
files directly when persistence is requested. Do not maintain separate manual
include or exclude lists.

## Dependency Semantics

If object A references B, A depends on B. B is upstream of A and must be
available before A for load planning.

Domain dependency rules:

- a domain's upstream domains must be available before that domain can be
  migrated
- downstream domains depend on this domain
- objects with no upstream dependencies belong in the first load tier
- objects that depend only on first-tier objects belong in the next load tier
- cross-domain dependencies must be listed explicitly

Do not describe load tiers using incoming-edge wording unless the graph direction
is explicitly reversed. Prefer "no upstream dependencies" for first-tier objects.

## Output Report

The report should include:

- summary of domains found
- object counts by domain and role
- setup-source candidates by domain
- upstream and downstream domain dependencies
- cross-domain dependencies
- ambiguous or unclassified objects
- conflicts requiring user decisions
- one JSON object per domain

## Domain File Contract

When the user explicitly asks to persist the analysis, write one file per domain:

```text
warehouse-catalog/data-domains/<slug>.json
```

Each file is the canonical current state for that domain.

Required fields:

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

Example:

```json
{
  "schema_version": 1,
  "domain": "Sales",
  "slug": "sales",
  "status": "candidate",
  "description": "Revenue and order lifecycle tables.",
  "confidence": "medium",
  "objects": {
    "tables": ["silver.fact_sales"],
    "views": ["gold.vw_sales_summary"],
    "procedures": ["silver.usp_load_fact_sales"],
    "functions": []
  },
  "setup_source_candidates": {
    "schemas": ["silver", "gold"],
    "tables": ["silver.fact_sales"]
  },
  "dependencies": {
    "upstream_domains": ["Customer", "Product"],
    "downstream_domains": ["Finance"]
  },
  "ambiguities": [
    {
      "object": "silver.transaction_log",
      "reason": "Name suggests either staging or fact-like event data."
    }
  ],
  "rationale": [
    "Objects use sales, order, invoice, and revenue terminology."
  ]
}
```

## Persistence Rules

Persist only when the user explicitly asks.

Rules:

- write only under `warehouse-catalog/data-domains/`
- do not write under `catalog/`
- do not create or populate `warehouse-ddl/`
- write complete canonical files, not patches
- rewrite only impacted domain files
- sort arrays before writing
- keep JSON field order stable
- write no volatile timestamps
- the same accepted state serializes to the same JSON
- each object has exactly one primary domain
- duplicate primary assignments are conflicts that require user resolution

If a domain becomes empty, keep the file with empty `objects` unless the user
explicitly asks to remove it.

## Reference Files

Load these only when needed:

| File | When to Load |
|---|---|
| `references/22_dw_table_patterns.md` | Dimensional role classification |
| `references/21_domain_taxonomy.md` | Business-domain assignment |
