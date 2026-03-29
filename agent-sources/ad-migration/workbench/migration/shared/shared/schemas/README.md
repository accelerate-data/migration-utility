# Catalog Schemas

Canonical schemas for the per-object catalog JSON files produced by `setup-ddl` and consumed by `discover`, `profile`, and downstream agents/skills.

## File layout

```text
<ddl-output-dir>/
├── tables.sql
├── procedures.sql
├── views.sql
├── functions.sql
└── catalog/
    ├── tables/
    │   ├── <schema>.<table>.json      → table_catalog.json schema
    │   └── ...
    ├── procedures/
    │   ├── <schema>.<proc>.json       → procedure_catalog.json schema
    │   └── ...
    ├── views/
    │   ├── <schema>.<view>.json       → view_catalog.json schema
    │   └── ...
    └── functions/
        ├── <schema>.<function>.json   → function_catalog.json schema
        └── ...
```

## Data sources

| Section | Source | When captured |
|---|---|---|
| Catalog signals (PKs, FKs, identity, CDC, sensitivity) | `sys.*` catalog views (bulk SELECTs) | setup-ddl |
| `references` / `referenced_by` (DMF-sourced) | `sys.dm_sql_referenced_entities` per proc/view/function | setup-ddl |
| `references` / `referenced_by` (AST-augmented) | sqlglot scan of proc bodies for CTAS, SELECT INTO, EXEC targets | setup-ddl |
| `profile` section | `/profile` skill or profiler agent | After setup-ddl, during profiling |

## Detection field

Entries in `references` and `referenced_by` carry a `detection` field when AST-augmented:

- Absent or `"dmf"` — from `sys.dm_sql_referenced_entities`. High trust.
- `"ast_scan"` — from sqlglot scan of proc bodies. Fills DMF gaps (CTAS, SELECT INTO, EXEC chains).

## Scope field

Every `references` and `referenced_by` group splits entries into:

- `in_scope` — object is within the extracted schemas. Full metadata available.
- `out_of_scope` — cross-database or cross-server reference. Only name/database/reason captured.

## Schemas

- [table_catalog.json](table_catalog.json) — per-table catalog file
- [procedure_catalog.json](procedure_catalog.json) — per-procedure catalog file
- [view_catalog.json](view_catalog.json) — per-view catalog file
- [function_catalog.json](function_catalog.json) — per-function catalog file
