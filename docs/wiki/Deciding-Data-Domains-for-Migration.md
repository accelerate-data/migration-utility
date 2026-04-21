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

In Claude Code, ask for the domain decision in plain language:

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
- object counts by domain and warehouse role
- setup-source candidates by domain
- upstream and downstream domain dependencies
- cross-domain dependencies, especially views over another domain's tables
- unresolved ownership questions that need a human decision

When you ask to persist the result, the project writes one canonical JSON file
per domain:

```text
warehouse-catalog/data-domains/<domain-slug>.json
```

Each table or view has exactly one primary business domain. Roles such as
staging, fact, dimension, aggregate, and reference describe the warehouse object
type; they are not business domains by themselves.

Procedures and functions can be inspected for dependency evidence, but they are
not domain-catalog objects.

## Ownership rules

Direct joins and aggregate tables can indicate that objects belong together in a
domain. Upstream or downstream lineage alone does not decide ownership.

A domain-specific view can belong to a different domain than its source table.
For example, a Sales table can remain in the Sales domain while a Finance
revenue view over that table belongs to Finance. In that case, record a
cross-domain dependency from Finance to Sales.

If an object has ambiguous ownership, stop and ask for the business decision
instead of guessing.

## Using the result

Use the domain files to decide which subset of the warehouse to migrate first.
For each chosen domain, review its setup-source candidates and then continue
with the normal source setup, scoping, target setup, and mart migration flow.

```text
warehouse-ddl/
  Whole-warehouse DDL snapshot
        |
        v
Decide data domains
  Business ownership + warehouse roles
        |
        v
warehouse-catalog/data-domains/
  One JSON file per domain
        |
        v
Choose migration domain
        |
        v
Review setup-source candidates
  Confirm source tables and seed tables
        |
        v
setup-source -> scoping -> mart migration flow
```

If ownership changes after review, update the canonical domain files directly.
Do not keep separate manual include or exclude lists for domain ownership.

## Related pages

- [[Quickstart]]
- [[Scoping]]
- [[Whole-Mart Migration]]
