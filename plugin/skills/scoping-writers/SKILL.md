---
name: scoping-writers
description: Reference content for the Scoping Agent — I/O schemas, resolution rules, validation checklist, and technology-specific write detection, call graph, scoring, and cross-database patterns. Load when running as the scoping agent or when detecting writer procedures, scoring candidates, or resolving CandidateWriters output.
user-invocable: false
---

# Scoping Writers

Reference content for the Scoping Agent. Load the section relevant to the current step.

## Shared (all technologies)

- **I/O schemas and diagnostics**: [reference/io-schema.md](reference/io-schema.md)
- **Resolution rules**: [reference/resolution.md](reference/resolution.md)
- **Validation checklist**: [reference/validation.md](reference/validation.md)

## T-SQL sources (SQL Server, Fabric Warehouse)

- **Write detection**: [reference/tsql/write-detection.md](reference/tsql/write-detection.md)
- **Call graph patterns**: [reference/tsql/call-graph.md](reference/tsql/call-graph.md)
- **Confidence scoring**: [reference/tsql/scoring.md](reference/tsql/scoring.md)
- **Cross-database patterns**: [reference/tsql/cross-db.md](reference/tsql/cross-db.md)
