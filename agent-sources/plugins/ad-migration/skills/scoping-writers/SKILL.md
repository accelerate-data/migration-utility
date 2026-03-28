---
name: scoping-writers
description: Reference content for the Scoping Agent — I/O schemas, resolution rules, validation checklist, and technology-specific write detection, call graph, scoring, and cross-database patterns. Load when running as the scoping agent or when detecting writer procedures, scoring candidates, or resolving CandidateWriters output.
user-invocable: false
---

> **Note:** `scope.py` and the scoping agent implement the same algorithm independently. `scope.py` is a standalone CLI for local use; the scoping agent uses DDL MCP tools and follows this skill's reference docs. Keep both in sync when the algorithm changes.

# Scoping Writers

Reference content for the Scoping Agent. Load the section relevant to the current step.

## Shared (all technologies)

- **I/O schemas and diagnostics**: [reference/io-schema.md](reference/io-schema.md)
- **Resolution rules**: [reference/resolution.md](reference/resolution.md)
- **Validation checklist**: [reference/validation.md](reference/validation.md)

## T-SQL sources (SQL Server, Fabric Warehouse)

- **DiscoverCandidates**: [reference/tsql/discover-candidates.md](reference/tsql/discover-candidates.md)
- **ResolveCallGraph**: [reference/tsql/resolve-call-graph.md](reference/tsql/resolve-call-graph.md)
- **Write detection**: [reference/tsql/write-detection.md](reference/tsql/write-detection.md)
- **Confidence scoring**: [reference/tsql/scoring.md](reference/tsql/scoring.md)
- **Call graph patterns**: [reference/tsql/call-graph.md](reference/tsql/call-graph.md)
- **Cross-database patterns**: [reference/tsql/cross-db.md](reference/tsql/cross-db.md)
