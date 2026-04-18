# Target-Normalized Catalog Types

## Decision

Catalog extraction writes three type fields for every extracted column:

- `source_sql_type`: source-native type string
- `canonical_tsql_type`: normalized T-SQL vocabulary type
- `sql_type`: selected target technology type

Extraction requires target technology to be present in `manifest.json`.

## Reason

Target table materialization, generated source YAML, staging model YAML, dbt contracts, and LLM-facing migration contexts need a target-adapter-compatible type. Sandbox/source materialization and source-facing diagnostics still need the original source-native type.

T-SQL is the canonical type vocabulary because SQL Server is the dominant source and already matches the common extracted type shape. A canonical middle type avoids maintaining pairwise source-to-target mappings for every technology combination.

## Mapping Contract

Type translation is two-stage:

```text
source technology + source metadata
  -> canonical T-SQL type
  -> target technology type
```

The mapper uses a structured internal representation before rendering catalog strings. It must preserve precision, scale, length, unicode-ness, binary-ness, timezone semantics, and nullability where the target adapter supports them.

Unsupported source-to-canonical mappings are not guessed. If a source type cannot map to canonical T-SQL, extraction records a table-level `TYPE_MAPPING_UNSUPPORTED` error and does not emit fallback canonical or target types for that column.

Unsupported canonical-to-target mappings are also blocking errors. They should identify the canonical type, target technology, and affected column.

## Catalog Contract

Column metadata keeps `source_sql_type` as the source-native extraction fact, `canonical_tsql_type` as the cross-technology normalized fact, and `sql_type` as the target generation fact.

All three fields are required for newly extracted table catalog entries when mapping succeeds. `sql_type` is the working column type exposed to target-facing commands, skills, and generated artifacts. `source_sql_type` and `canonical_tsql_type` are retained for deterministic source handling and debugging, not for LLM prompts.

Type mapping errors are stored in the existing top-level table catalog `errors` list using the normal diagnostic shape.

## Usage Boundary

Sandbox/source materialization, source inspection, lineage, and source-facing diagnostics read `source_sql_type` when they need the extracted source-native type.

Target source table materialization, generated source YAML `data_type`, staging model YAML `data_type`, dbt contract `data_type`, and LLM-facing context payloads read `sql_type`.

Profile, discover, refactor, migrate, model generation, and test generation contexts show only `sql_type` to avoid presenting multiple data types to the LLM. Diagnostics may show `source_sql_type` and `canonical_tsql_type` to explain how a source type became a target type.

Executable comparison and sandbox workflows own any source-vs-target type switch at the harness or backend boundary. Compare-sql should not require LLM-facing contexts to include source-native column types.

## Readiness Boundary

`VU-1119` owns making object-scoped `migrate-util ready` fail when a catalog object has unresolved errors. `VU-1117` depends on that behavior so skills stop naturally through their existing readiness guard when type mapping errors are present.

`VU-1119` owns target-technology extraction blocking, type-mapping error writeback, and readiness/status blocking for those errors. `VU-1117` assumes those behaviors are present and owns the successful-mapping catalog column contract plus target-only consumer visibility.

## Reruns

Rerunning extraction refreshes `source_sql_type`, `canonical_tsql_type`, and `sql_type` from current source metadata and current target technology.
