# Target-Normalized Catalog Types

## Decision

Catalog extraction writes three type fields for every extracted column:

- `sql_type`: source-native type string
- `canonical_tsql_type`: normalized T-SQL vocabulary type
- `target_sql_type`: selected target technology type

Extraction requires target technology to be present in `manifest.json`.

## Reason

Target table materialization, generated source YAML, staging model YAML, and dbt contracts need a target-adapter-compatible type. Sandbox/source materialization and source-facing diagnostics still need the original source-native type.

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

Column metadata keeps `sql_type` as the source-native extraction fact, `canonical_tsql_type` as the cross-technology normalized fact, and `target_sql_type` as the target generation fact.

All three fields are required for newly extracted table catalog entries when mapping succeeds. Type mapping errors are stored in the existing top-level table catalog `errors` list using the normal diagnostic shape.

`TYPE_MAPPING_UNSUPPORTED` is registered in the diagnostic stage map so `/status` marks the affected stage as `error` and later stages as `blocked`.

## Usage Boundary

Sandbox/source materialization, source inspection, lineage, and user-facing source diagnostics read `sql_type`.

Target source table materialization, generated source YAML `data_type`, staging model YAML `data_type`, and dbt contract `data_type` read `target_sql_type`.

Diagnostics may show `canonical_tsql_type` to explain how a source type became a target type.

## Readiness Boundary

`VU-1119` owns making object-scoped `migrate-util ready` fail when a catalog object has unresolved errors. `VU-1117` depends on that behavior so skills stop naturally through their existing readiness guard when type mapping errors are present.

`VU-1117` still owns writing type mapping errors into the table catalog and making setup-target fail fast for affected source tables, because setup-target materializes target source tables directly from catalog metadata.

## Reruns

Rerunning extraction refreshes `sql_type`, `canonical_tsql_type`, and `target_sql_type` from current source metadata and current target technology.
