# Deciding Data Domains for Migration

Use this operation when a warehouse has many tables or views and you need to
split them into business-owned migration domains before choosing source scope or
planning mart migration work.

This is most useful for monolithic warehouses, shared reporting databases, and
lakehouse zones where schemas or table prefixes do not cleanly map to business
ownership.

## When to use it

Run domain decision before per-object migration when you need to answer:

- which business domain owns each table or view
- which tables are good setup-source candidates for each domain
- which views depend on objects from another domain
- which domains should be migrated before others
- which ownership choices need human confirmation

Skip this operation for a small, already-scoped migration repo where the source
tables and target mart are known.

## Required input

Start from a whole-warehouse DDL snapshot. The project should contain
`warehouse-ddl/` with the tables and views to classify.

This operation is separate from the normal one-domain extraction under `ddl/`
and `catalog/`. The whole-warehouse domain catalog lives under
`warehouse-catalog/`.

## Claude Code prompt

In Claude Code, invoke the `classifying-data-domains` skill by asking for the
domain decision in plain language:

```text
Decide data domains for this warehouse and persist the result.
```

For a first review without writing files:

```text
Decide data domains for this warehouse. Do not persist files yet.
```

When you already know an ownership correction:

```text
Move shared.opportunity_cases to the Operations domain and persist the updated
domain catalog.
```

## Output

The response should include:

- a summary of domains found
- layer counts for source, staging, ODS, warehouse, and ETL-control objects
- object counts by domain and table classification
- upstream and downstream domain dependencies
- unresolved layer or domain questions that need a human decision
- classification decisions that can be handled later during mart migration

When you ask to persist the result, the project writes decision files:

```text
warehouse-catalog/domains/
  sales.json
  finance.json
  operations.json
```

Each file is the canonical accepted state for one business domain, including
domain-owned tables and views, setup-source candidates, dependencies,
ambiguities, and rationale.

Procedures and functions can be inspected for dependency evidence, but they are
not domain-catalog objects.

## Ownership rules

Domain grouping starts from fact tables. Direct joins can indicate that objects
belong together in a domain. Upstream or downstream lineage alone does not
decide ownership.

Aggregates inherit domain from the fact or dimension they aggregate.
Minidimensions and bridge tables inherit domain from the dimension they support.
Conformed dimensions belong to the subject domain when that subject is clear.

A same-grain derived fact stays with the base fact's domain unless it adds
domain-specific business semantics or enrichment. If ownership is unclear, leave
the domain decision unresolved for human review.

Date dimensions are resolved as a shared canonical date dimension and included
as a required shared dimension for generated domain outputs. Date dimensions do
not count toward domain-size warnings.

Junk dimensions may be classified from DDL, but domain ownership usually remains
unresolved because DDL does not show value-level ownership.

## Using the result

Use generated domain reports to decide which subset of the warehouse to migrate
first. For each chosen domain, review unresolved placement decisions and then
continue with the normal source setup, scoping, target setup, and mart migration
flow.

```text
warehouse-ddl/
  Whole-warehouse DDL snapshot
        |
        v
Decide data domains
  Layer + domain + table classification decisions
        |
        v
warehouse-catalog/domains/
  One JSON file per business domain
        |
        v
Choose migration domain
        |
        v
Review generated domain report
  Confirm unresolved layer/domain decisions
        |
        v
setup-source -> scoping -> mart migration flow
```

If ownership changes after review, update the relevant decision entry and rerun
domain evaluation so inherited assignments and reports are regenerated.

## Related pages

- [[Quickstart]]
- [[Scoping]]
- [[Whole-Mart Migration]]
