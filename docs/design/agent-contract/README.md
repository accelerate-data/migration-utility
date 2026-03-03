# Agent Contract

Contracts for multi-agent ETL migration from SQL Server stored procedures to dbt models.

## Flow

1. Scoping: scoping agent maps target table to writer procedure candidate(s).
2. Profiling: profiler agent gathers SQL Server facts and emits a deterministic profile JSON.
3. Planning: planner agent consumes scope input + profile JSON and emits editable plan JSON for FDE review.

## Contract Pages

- [Scoping Agent Contract](scoping-agent.md) - input/output contract for table-to-writer procedure mapping.
- [Profiler Agent Contract](profiler-agent.md) - required input and output schema for SQL Server profiling.
- [Planner Agent Contract](planner-agent.md) - required input and output schema for dbt model planning.

## Contract Boundary

- Scoping output is writer candidate discovery and ranking.
- Profiler output is facts/evidence only.
- Planner output is decisions/proposal only.
- Planner should not re-query SQL Server when profile completeness is `complete` and required sections are present.
