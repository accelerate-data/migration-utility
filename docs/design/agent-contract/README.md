# Agent Contract

Contracts for multi-agent ETL migration from SQL Server stored procedures to dbt models.

All contracts are batch-only. Single-table UI execution is a degenerate batch with one `items[]` element.

## Flow

1. Scoping: scoping agent maps target table to writer procedure candidate(s) and selects writer when resolvable.
2. Profiling: profiler agent proposes candidate migration decisions for FDE approval.
3. Planning: planner agent captures FDE-approved decisions and documentation intent.
4. Migration: migrator agent converts approved decisions into dbt artifacts using tool-fetched facts.

## Contract Pages

- [Scoping Agent Contract](scoping-agent.md) - input/output contract for table-to-writer procedure mapping.
- [Profiler Agent Contract](profiler-agent.md) - required input and output schema for candidate generation.
- [Planner Agent Contract](planner-agent.md) - required input and output schema for decision manifest generation.
- [Migrator Agent Contract](migrator-agent.md) - required input and output schema for dbt artifact generation.

## Contract Boundary

- Scoping output is minimal writer discovery/selection data.
- Profiler output is candidate proposals that require FDE judgment.
- Planner captures approved decisions and required documentation metadata.
- Migrator fetches direct facts via tools and materializes dbt files from approved decisions.
- Upstream agents should not emit fields that downstream stages can derive or fetch reliably.

## Workflow Semantics

- Scoping and profiling produce machine-readable outputs with per-item status and errors.
- FDE approval is the decision gate between profiler candidates and migrator execution.
- Validation findings should be surfaced as structured warnings/errors.
