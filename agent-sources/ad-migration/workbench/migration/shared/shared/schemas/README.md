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

Per-object catalog files produced by `setup-ddl` and consumed by `discover`, `profile`, and downstream agents/skills.

| Schema | Object type | Key fields |
|---|---|---|
| [table_catalog.json](table_catalog.json) | Table | PKs, FKs, identity, CDC, sensitivity, `referenced_by`, `profile` |
| [procedure_catalog.json](procedure_catalog.json) | Procedure | `references`, `referenced_by` |
| [view_catalog.json](view_catalog.json) | View | `references`, `referenced_by` |
| [function_catalog.json](function_catalog.json) | Function | `references`, `referenced_by` |

### File layout

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

### Data sources

| Section | Source | When captured |
|---|---|---|
| Catalog signals (PKs, FKs, identity, CDC, sensitivity) | `sys.*` catalog views (bulk SELECTs) | setup-ddl |
| `references` / `referenced_by` (DMF-sourced) | `sys.dm_sql_referenced_entities` per proc/view/function | setup-ddl |
| `references` / `referenced_by` (AST-augmented) | sqlglot scan of proc bodies for CTAS, SELECT INTO, EXEC targets | setup-ddl |
| `profile` section | `/profile` skill or profiler agent | After setup-ddl, during profiling |

### Detection field

Entries in `references` and `referenced_by` carry a `detection` field when AST-augmented:

- Absent or `"dmf"` -- from `sys.dm_sql_referenced_entities`. High trust.
- `"ast_scan"` -- from sqlglot scan of proc bodies. Fills DMF gaps (CTAS, SELECT INTO, EXEC chains).

### Scope field

Every `references` and `referenced_by` group splits entries into:

- `in_scope` -- object is within the extracted schemas. Full metadata available.
- `out_of_scope` -- cross-database or cross-server reference. Only name/database/reason captured.

## Agent input schemas

| Schema | Agent | Required fields |
|---|---|---|
| [scope_input.json](scope_input.json) | Scoping | `schema_version`, `run_id`, `technology`, `ddl_path`, `items[].item_id` |
| [profiler_input.json](profiler_input.json) | Profiler | `schema_version`, `run_id`, `ddl_path`, `items[].item_id`, `items[].selected_writer` |
| [decomposer_input.json](decomposer_input.json) | Decomposer | `schema_version`, `run_id`, `items[].item_id`, `items[].writer` |
| [planner_input.json](planner_input.json) | Planner | `schema_version`, `run_id`, `items[].item_id`, `items[].answers`, `items[].decomposition` |
| [test_generator_input.json](test_generator_input.json) | Test Generator | `schema_version`, `run_id`, `items[].item_id`, `items[].answers`, `items[].decomposition`, `items[].plan` |
| [migrator_input.json](migrator_input.json) | Migrator | `schema_version`, `run_id`, `items[].item_id`, `items[].answers`, `items[].decomposition`, `items[].plan`, `items[].unit_tests` |

## Agent output schemas

| Schema | Agent | Notes |
|---|---|---|
| [candidate_writers.json](candidate_writers.json) | Scoping | Per-table writer discovery with `status`, `selected_writer`, `candidate_writers[]`, custom `scope_summary` |
| [decomposition_proposal.json](decomposition_proposal.json) | Decomposer | Logical blocks and split points per item |
| [planner_design_manifest.json](planner_design_manifest.json) | Planner | Materialization, schema tests, documentation per item |
| [fixture_manifest.json](fixture_manifest.json) | Test Generator | Branch-covering unit test fixtures with coverage tracking |
| [migration_artifact_manifest.json](migration_artifact_manifest.json) | Migrator | Generated dbt artifact paths, metadata, and execution results |

## Skill output schemas

| Schema | Skill / Command | Notes |
|---|---|---|
| [profile_context.json](profile_context.json) | `profile.py context` | Deterministic context assembly for LLM profiling: table, writer, catalog signals, writer references, proc body, columns, related procedures |
