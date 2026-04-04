# Schemas

Canonical JSON Schema files for catalog objects, agent input/output contracts, and skill outputs. All schemas use JSON Schema draft 2020-12.

## Common shared definitions

[common.json](common.json) contains `$defs` shared across multiple schemas:

| Definition | Description |
|---|---|
| `scoped_ref_list` | `{in_scope: [], out_of_scope: []}` reference grouping |
| `in_scope_ref` | In-scope reference entry with `is_selected`, `is_updated`, `columns`, etc. |
| `out_of_scope_ref` | Cross-database/server reference entry with `reason` |
| `column_ref` | `{name, is_selected, is_updated}` |
| `diagnostics_entry` | `{code, message, field, severity, details}` used in `validation.issues[]`, `warnings[]`, `errors[]` |
| `validation_section` | `{passed: bool, issues: []}` |
| `summary_section` | `{total, ok, partial, error}` counts |

Catalog and agent schemas reference these via `$ref: "common.json#/$defs/<name>"`.

## Catalog schemas

Per-object catalog files produced by `setup-ddl` and consumed by `listing-objects`, `scoping-table`, `profiling-table`, and downstream agents/skills.

| Schema | Object type | Key fields |
|---|---|---|
| [manifest.json](manifest.json) | Extraction manifest | technology, dialect, source_database, extracted_schemas, extracted_at |
| [table_catalog.json](table_catalog.json) | Table | `columns`, PKs, FKs, auto_increment_columns, change_capture (opt), sensitivity (opt), `referenced_by`, `profile`, `scoping` |
| [procedure_catalog.json](procedure_catalog.json) | Procedure | `params`, `references`, `referenced_by`, `statements`, `needs_llm`, `needs_enrich`, `mode`, `routing_reasons` |
| [view_catalog.json](view_catalog.json) | View | `references`, `referenced_by` |
| [function_catalog.json](function_catalog.json) | Function | `references`, `referenced_by` |

### File layout

```text
<ddl-output-dir>/
├── manifest.json              → manifest.json schema
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

### Data sources

| Section | Source | When captured |
|---|---|---|
| Catalog signals (PKs, FKs, auto_increment_columns, change_capture, sensitivity) | `sys.*` catalog views (bulk SELECTs) | setup-ddl |
| `references` / `referenced_by` (catalog-query-sourced) | `sys.dm_sql_referenced_entities` per proc/view/function | setup-ddl |
| `references` / `referenced_by` (AST-augmented) | sqlglot scan of proc bodies for CTAS, SELECT INTO, EXEC targets | setup-ddl |
| `profile` section | `/profiling-table` skill or profiler agent | After setup-ddl, during profiling |
| `scoping` section | scoping agent or `/scoping-table` | After setup-ddl, during scoping |
| manifest.json | Written by setup-ddl at extraction time | setup-ddl |

### Detection field

Entries in `references` and `referenced_by` carry a `detection` field when AST-augmented:

- Absent or `"catalog_query"` -- from `sys.dm_sql_referenced_entities`. High trust.
- `"ast_scan"` -- from sqlglot scan of proc bodies. Fills DMF gaps (CTAS, SELECT INTO, EXEC chains).

### Scope field

Every `references` and `referenced_by` group splits entries into:

- `in_scope` -- object is within the extracted schemas. Full metadata available.
- `out_of_scope` -- cross-database or cross-server reference. Only name/database/reason captured.

## discover CLI output schemas

Structured JSON output from the `discover` CLI subcommands, consumed by skills and agents.

| Schema | Subcommand | Notes |
|---|---|---|
| [discover_list_output.json](discover_list_output.json) | `discover list` | `{objects: [string]}` |
| [discover_show_output.json](discover_show_output.json) | `discover show` | columns, params, refs (from catalog), statements (AST, deterministic only), classification |
| [discover_refs_output.json](discover_refs_output.json) | `discover refs` | readers, writers with `is_updated`/`is_selected` from catalog. No confidence scoring. |

## Agent input schemas

| Schema | Agent | Required fields |
|---|---|---|
| [profiler_input.json](profiler_input.json) | Profiler | `schema_version`, `run_id`, `items[].item_id` |
| [model_generator_input.json](model_generator_input.json) | Model Generator | `schema_version`, `run_id`, `items[].item_id` |

## Agent output schemas

| Schema | Agent | Notes |
|---|---|---|
| [scoping_summary.json](scoping_summary.json) | Scoping | Lightweight batch rollup with per-item `status` — full scoping data lives in `catalog/tables/<table>.json` |
| [fixture_manifest.json](fixture_manifest.json) | Test Generator | Branch-covering unit test fixtures with coverage tracking |
| [migration_artifact_manifest.json](migration_artifact_manifest.json) | Migrator | Generated dbt artifact paths, metadata, and execution results |

## Skill output schemas

| Schema | Skill / Command | Notes |
|---|---|---|
| [profile_context.json](profile_context.json) | `profile.py context` | Deterministic context assembly for LLM profiling: table, writer, catalog signals, writer references, proc body, columns, related procedures |
| [test_spec.json](test_spec.json) | `/generating-tests` skill | Per-item test spec written to `test-specs/<item_id>.json`: branch manifest, unit tests with fixtures and ground truth, coverage status |
| [sandbox_status_output.json](sandbox_status_output.json) | `test-harness sandbox-status` | Sandbox existence check result: database name, exists boolean |
