# Catalog Enrichment Diagnostics

This document covers only the diagnostics and warnings layer of catalog enrichment. The enrichment implementation lives in `plugin/lib/shared/catalog_enrich.py`, `plugin/lib/shared/setup_ddl.py`, and `plugin/lib/shared/setup_ddl_support/`.

Exhaustive catalog of diagnostic scenarios for `warnings[]` and `errors[]` arrays on view, function, and procedure catalog entries. Each scenario is detectable via static AST analysis (sqlglot) and/or catalog state inspection — no LLM, no live DB queries required.

Parent issue: VU-917 (schema changes). This document: VU-918 (investigation).

## Overview

### Severity Model

| Level | Meaning | Migration effect |
|---|---|---|
| **error** | Object cannot be migrated without human intervention | Pipeline halts or skips the object |
| **warning** | Object can be migrated but the result may be incorrect or suboptimal | Pipeline proceeds; user should review |

### Diagnostic Code Format

Flat `UPPER_SNAKE_CASE` identifiers reused across object types where the same condition applies. The catalog file path (`catalog/views/`, `catalog/functions/`, `catalog/procedures/`) disambiguates the object type.

### Schema Reference

Every diagnostic entry conforms to `common.json#/$defs/diagnostics_entry`:

```json
{
  "code": "PARSE_ERROR",
  "message": "Human-readable description.",
  "item_id": "schema.object_name",
  "field": "optional.field.path",
  "severity": "error",
  "details": {}
}
```

### Detection Phase

All diagnostics fire during catalog enrichment (`catalog_enrich.py`) or catalog writing (`write_object_catalog()` in `catalog.py`). They consume data already extracted by `setup_ddl` / `sqlserver_extract` / `oracle_extract` and parsed by `loader_parse.py`.

## View Diagnostics

### Parse and Structure

#### PARSE_ERROR

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Both |
| Trigger | sqlglot cannot parse the view DDL: `ast=None`, `parse_error` field is set on the `DdlEntry`. Also fires when sqlglot returns an `exp.Command` node instead of a structured AST. |
| Current state | `parse_error` string is stored on the `DdlEntry` in `loader_io.py` (line 128) but never surfaced as a `warnings[]`/`errors[]` entry on the view catalog JSON. |
| User impact | No AST-based references can be extracted. The view's `references` section relies entirely on DMF data, which may be incomplete or misclassified. Downstream migration steps that depend on AST analysis will silently skip this view. |
| Remediation | Inspect the raw DDL for unsupported syntax. Simplify or rewrite the view to use constructs sqlglot can parse. If the view uses dialect-specific syntax (e.g., Oracle `CONNECT BY`, `MODEL`, `XMLTABLE`), consider manual reference annotation. |
| Details | `{"parse_error": "<sqlglot error message>"}` |

#### UNSUPPORTED_SYNTAX

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | sqlglot parses the DDL without raising an exception but returns an `exp.Command` node for part of the body. The overall parse succeeds but subtrees are opaque. |
| Current state | `parse_block()` in `loader_parse.py` raises `DdlParseError` when the top-level result is a `Command`, but individual nested `Command` nodes within an otherwise valid AST are not flagged. |
| User impact | References within the opaque subtree are missed. The view appears parseable but has blind spots. |
| Remediation | Review the specific SQL construct that produced the `Command` node. File a sqlglot issue if the construct should be supported. |
| Details | `{"command_text": "<raw text of the Command node>"}` |

#### DUPLICATE_DEFINITION

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | The same view name (normalized FQN) appears more than once in the DDL file(s). The second definition silently overwrites the first in the catalog dict during `_load_file()`. |
| Current state | No warning is emitted. Oracle DDL files commonly contain `CREATE OR REPLACE VIEW` before procedure definitions (observed in `test-fixtures/procedures/oracle.sql`). |
| User impact | The earlier definition is lost. If the two definitions differ, the catalog reflects whichever was loaded last, with no indication that an overwrite occurred. |
| Remediation | Deduplicate the DDL file. If both definitions are intentional (e.g., environment-specific), split into separate files or use conditional logic at the source. |
| Details | `{"occurrences": 2, "file": "views.sql"}` |

#### STALE_OBJECT

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | The view existed in a previous extraction but is absent from the current source. `_mark_stale()` in `catalog.py` sets `stale: true` on the catalog JSON. |
| Current state | The `stale` field is set but no `warnings[]` entry is created. Stale views are returned by `list_views` and `get_view_body` with no visual distinction. |
| User impact | Downstream consumers (skills, commands, MCP clients) may reference a view that no longer exists at the source. Migration plans built on stale catalog data will fail at execution time. |
| Remediation | Re-run `setup-ddl` to refresh the catalog. If the view was intentionally dropped, remove its catalog file manually or let the next extraction mark it stale. |
| Details | `{"previous_ddl_hash": "<sha256>"}` |

### Reference Resolution

#### MISSING_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | The view's body references a table, view, or function (via AST or DMF) that has no corresponding catalog file. The referenced FQN is not found in `catalog/tables/`, `catalog/views/`, or `catalog/functions/`. |
| Current state | Missing references land in `out_of_scope` with reason `cross_database_reference` or `cross_server_reference` if the DMF flagged them, but locally-missing references (same database, same schema batch, but no catalog file) produce no diagnostic. |
| User impact | The migration output for this view may reference a table that doesn't exist in the dbt project. The generated model will fail `dbt run` with a missing ref. |
| Remediation | Ensure the referenced object is included in the extraction scope. If it is intentionally excluded (e.g., a system table), document the exclusion. |
| Details | `{"missing_fqn": "schema.object", "reference_type": "table\|view\|function"}` |

#### OUT_OF_SCOPE_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | The view references an object classified as `out_of_scope` by DMF — either `cross_database_reference` (different database) or `cross_server_reference` (linked server). |
| Current state | Out-of-scope references are stored in `references.*.out_of_scope` with a reason field. No `warnings[]` entry is created. |
| User impact | The view depends on an external object that will not exist in the target dbt project. The migration must handle this dependency manually (e.g., a source definition or external table). |
| Remediation | Create a dbt `source` definition for the external object, or replace the cross-database reference with a local equivalent in the target environment. |
| Details | `{"fqn": "server.database.schema.object", "reason": "cross_database_reference\|cross_server_reference"}` |

#### AMBIGUOUS_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF returns `is_ambiguous=true` for a reference. SQL Server sets this flag when column-level resolution cannot determine the exact source table for a column reference (e.g., `SELECT col FROM a JOIN b` where `col` exists in both `a` and `b`). |
| Current state | The `is_ambiguous` flag is stored in the catalog JSON by `dmf_processing.py` but never surfaced as a diagnostic or in any CLI/MCP output. |
| User impact | Column lineage is unreliable for this reference. Profiling or model generation that depends on column-level tracking may attribute columns to the wrong source table. |
| Remediation | Qualify the ambiguous column reference in the view definition (e.g., `a.col` instead of `col`). |
| Details | `{"reference_fqn": "schema.table", "ambiguous_columns": ["col1", "col2"]}` |

#### DMF_MISCLASSIFIED

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF returns `referenced_class_desc = "OBJECT_OR_COLUMN"` for a reference, and `_classify_referenced_type()` in `dmf_processing.py` cannot resolve the actual type from the `object_types` dict. The reference defaults to the `tables` bucket even though it may be a view or function. |
| Current state | The misclassification happens silently. Observed in test fixtures: `dbo.usp_loadfromview` has `silver.vw_ProductCatalog` in its `tables` bucket instead of `views`. |
| User impact | A view reference appears as a table reference. The view's `referenced_by` section is not populated. Dependency graphs are incorrect — the view appears unreferenced while a phantom table appears referenced. |
| Remediation | Post-extraction validation: cross-check `references.tables.in_scope` entries against `catalog/views/` and `catalog/functions/` to detect misclassified entries. Reclassify and update both the referencing and referenced catalog files. |
| Details | `{"misclassified_fqn": "schema.name", "assigned_bucket": "tables", "likely_bucket": "views\|functions"}` |

#### NESTED_VIEW_CHAIN

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A view references another view, which references another view, and so on, forming a chain of depth N or more (configurable threshold, suggested default: 5). Detected by walking the view reference graph (BFS/DFS from the view's `references.views.in_scope`). |
| Current state | No depth analysis is performed on view chains. `catalog_enrich.py` does not process views at all. |
| User impact | Deeply nested view chains are difficult to debug, may have performance implications at the source, and produce complex dbt model dependency graphs. Each intermediate view becomes a dbt model or ephemeral CTE, increasing compilation time and complexity. |
| Remediation | Consider flattening the view chain by inlining intermediate views. Review whether all intermediate views are necessary for the migration. |
| Details | `{"depth": 7, "chain": ["schema.v1", "schema.v2", "schema.v3", "..."]}` |

### Cross-Object and Transitive

#### DEPENDENCY_HAS_ERROR

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A dependency of this view (a referenced table, view, or function) has an error-level diagnostic in its own catalog. For example, the view references a function that has `PARSE_ERROR`. |
| Current state | No transitive diagnostic propagation exists. Each object's diagnostics are independent. |
| User impact | The view may parse and migrate successfully in isolation, but the generated model will fail because a dependency is broken. The user discovers the issue only at `dbt run` time, far from the root cause. |
| Remediation | Fix the error on the dependency first. The transitive warning clears automatically once the dependency's error is resolved. |
| Details | `{"dependency_fqn": "schema.object", "dependency_type": "view\|function\|table", "error_code": "PARSE_ERROR"}` |

#### TRANSITIVE_SCOPE_LEAK

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A dependency of this view has `OUT_OF_SCOPE_REFERENCE` or `MISSING_REFERENCE` diagnostics. The view itself has all references in scope, but its dependency tree includes objects outside the migration boundary. |
| Current state | No transitive scope analysis exists. |
| User impact | The migration appears complete for this view, but a dependency further down the chain references an external object. The `dbt run` may succeed (if the intermediate view is ephemeral and the external ref is a source), but data lineage is incomplete. |
| Remediation | Audit the full dependency tree. Ensure all transitive dependencies are either in scope or have explicit `source` definitions. |
| Details | `{"dependency_fqn": "schema.view_b", "leaked_reference": "other_db.schema.table", "leak_type": "out_of_scope\|missing"}` |

#### WRITER_THROUGH_VIEW

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A procedure writes to this view (DMF reports the view as the write target with `is_updated=true` in the procedure's references). The base table's `referenced_by` is not updated to reflect the indirect write. |
| Current state | DMF records the view as the write target. The view appears in the procedure's `references.views.in_scope` with `is_updated=true`, but the base table referenced by the view has no knowledge of this write path. |
| User impact | Table scoping (writer discovery) misses the indirect write. The table may not be selected for migration even though a procedure modifies it through the view. Migration completeness is compromised. |
| Remediation | During enrichment, follow the view-to-base-table link and propagate the `is_updated` flag to the base table's `referenced_by`. Flag the view so the user knows it participates in a write path. |
| Details | `{"writing_procedure": "schema.proc_name", "base_tables": ["schema.table1"]}` |

### Dialect-Specific

#### LONG_TRUNCATION (Oracle)

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Oracle |
| Trigger | `ALL_VIEWS.TEXT` is a LONG column. Oracle `oracledb` thin mode silently truncates LONG values at 32,767 bytes. The extraction code in `oracle_extract.py` (`_extract_view_ddl()`) detects exact truncation (`len(text) == 32767`) and falls back to `DBMS_METADATA.GET_DDL`. This diagnostic fires when the fallback also fails, or when the text length is between ~32,700 and 32,766 (off-by-one: truncated but not detected). |
| Current state | If the CLOB fallback succeeds, the view is extracted normally. If it fails, `event=oracle_view_skip` is logged and the view is omitted entirely — no catalog entry, no diagnostic. |
| User impact | The view is silently missing from the catalog. Any procedure or view that references it will have a `MISSING_REFERENCE` diagnostic, but the root cause (truncation) is hidden. |
| Remediation | Check the Oracle extraction log for `oracle_view_skip` events. For affected views, extract the DDL manually using `DBMS_METADATA.GET_DDL` in a SQL client and add it to the DDL directory. |
| Details | `{"text_length": 32767, "fallback_error": "<error message>"}` |

#### INVALID_SOURCE_OBJECT (Oracle)

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Oracle |
| Trigger | The Oracle object has `STATUS != 'VALID'` in `ALL_OBJECTS`. `_extract_object_types()` in `oracle_extract.py` filters to `STATUS = 'VALID'` only, so invalid objects are excluded from extraction entirely. |
| Current state | Invalid objects are silently excluded. No catalog entry is created. No diagnostic is emitted. |
| User impact | A view with a compilation error at the Oracle source is invisible to the migration pipeline. If a procedure references this view, the reference will appear as `MISSING_REFERENCE` with no indication that the view exists but is broken. |
| Remediation | Fix the compilation error at the Oracle source (`ALTER VIEW ... COMPILE`). Alternatively, extract the DDL manually for invalid objects and include them in the DDL directory with a manual annotation. |
| Details | `{"oracle_status": "INVALID", "object_type": "VIEW"}` |

#### MATERIALIZED_VIEW_AS_TABLE (Oracle)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Oracle |
| Trigger | Oracle materialized views have `OBJECT_TYPE = 'MATERIALIZED VIEW'` in `ALL_OBJECTS` but appear with type `"U"` (user table) in some metadata views. `_TYPE_MAPPING` in `setup_ddl.py` maps `"U"` to `"tables"`. The MV ends up in the tables catalog bucket instead of the views bucket. Meanwhile, `_build_oracle_schema_summary()` counts MVs under the views display row, creating a discrepancy. |
| Current state | MVs are silently treated as tables. The schema summary overcounts views. |
| User impact | The MV does not appear in `list_views`. Dependency analysis treats it as a table. If the migration target needs to recreate the MV as a dbt model (not a table), the wrong template or strategy may be applied. |
| Remediation | Post-extraction: identify MVs by checking `ALL_OBJECTS WHERE OBJECT_TYPE = 'MATERIALIZED VIEW'` and reclassify their catalog entries from tables to views. |
| Details | `{"oracle_object_type": "MATERIALIZED VIEW", "classified_as": "table"}` |

## Function Diagnostics

### Parse and Structure

#### PARSE_ERROR

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Both |
| Trigger | sqlglot cannot parse the function DDL: `ast=None`, `parse_error` field is set. Also fires when sqlglot returns an `exp.Command` node. Oracle `PIPELINED` table functions and functions using `PRAGMA` directives are common triggers. |
| Current state | `parse_error` is stored on the `DdlEntry` but never surfaced in the function catalog JSON. |
| User impact | No AST-based references can be extracted. The function's `references` section relies entirely on DMF. Migration steps that need to understand the function's logic (e.g., inlining a scalar function into a dbt model) cannot proceed. |
| Remediation | Inspect the raw DDL. For Oracle `PIPELINED` functions, consider manual reference annotation. For unsupported syntax, simplify or file a sqlglot issue. |
| Details | `{"parse_error": "<sqlglot error message>"}` |

#### UNSUPPORTED_SYNTAX

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | sqlglot parses the function DDL but returns `exp.Command` nodes for subtrees. The overall parse succeeds but some branches are opaque. |
| Current state | Not detected. Nested `Command` nodes are not flagged. |
| User impact | References within opaque subtrees are missed. |
| Remediation | Review the specific construct. File a sqlglot issue if needed. |
| Details | `{"command_text": "<raw text>"}` |

#### DUPLICATE_DEFINITION

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Same function name (normalized FQN) appears more than once in the DDL file(s). |
| Current state | No warning emitted. Second definition silently overwrites the first. |
| User impact | Earlier definition is lost. |
| Remediation | Deduplicate the DDL file. |
| Details | `{"occurrences": 2, "file": "functions.sql"}` |

#### STALE_OBJECT

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Function existed in previous extraction but is absent from current source. |
| Current state | `stale: true` is set but no `warnings[]` entry. |
| User impact | Downstream consumers may reference a function that no longer exists. |
| Remediation | Re-run `setup-ddl`. |
| Details | `{"previous_ddl_hash": "<sha256>"}` |

### Reference Resolution

#### MISSING_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Function references a table, view, or function not in the catalog. |
| Current state | No diagnostic. Missing references either land in `out_of_scope` or are absent entirely. |
| User impact | Generated models referencing this function may fail due to missing dependencies. |
| Remediation | Include the referenced object in extraction scope. |
| Details | `{"missing_fqn": "schema.object", "reference_type": "table\|view\|function"}` |

#### OUT_OF_SCOPE_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Cross-database or cross-server reference in the function. |
| Current state | Stored in `out_of_scope` with reason but no `warnings[]` entry. |
| User impact | Function depends on external object not in the dbt project. |
| Remediation | Create a dbt `source` or replace the external reference. |
| Details | `{"fqn": "server.db.schema.object", "reason": "cross_database_reference\|cross_server_reference"}` |

#### AMBIGUOUS_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF `is_ambiguous=true` on a reference within the function. |
| Current state | Flag stored but never surfaced. |
| User impact | Column lineage unreliable. |
| Remediation | Qualify the ambiguous column reference. |
| Details | `{"reference_fqn": "schema.table", "ambiguous_columns": ["col"]}` |

#### DMF_MISCLASSIFIED

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF `OBJECT_OR_COLUMN` couldn't resolve type; defaulted to `tables` bucket. |
| Current state | Silent misclassification. |
| User impact | View or function reference appears as a table. Dependency graph is incorrect. |
| Remediation | Cross-check `references.tables.in_scope` against view/function catalogs. |
| Details | `{"misclassified_fqn": "schema.name", "assigned_bucket": "tables", "likely_bucket": "views\|functions"}` |

### Cross-Object and Transitive

#### CIRCULAR_REFERENCE

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Both |
| Trigger | Function A references Function B, which references Function A (cycle in the function reference graph). Detected by BFS/DFS cycle detection on `references.functions.in_scope`. |
| Current state | No cycle detection exists for functions. `catalog_enrich.py` only processes procedures. |
| User impact | Circular function dependencies cause infinite recursion at runtime (SQL Server will error; Oracle may hang). The migration cannot produce a valid dbt dependency graph — dbt does not support circular refs. |
| Remediation | Break the cycle by refactoring one function to not reference the other. This is likely a bug at the source. |
| Details | `{"cycle": ["schema.fn_a", "schema.fn_b", "schema.fn_a"]}` |

#### DEPENDENCY_HAS_ERROR

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A dependency of this function has an error-level diagnostic. |
| Current state | No transitive propagation. |
| User impact | Function may work in isolation but fail due to a broken dependency. |
| Remediation | Fix the dependency's error first. |
| Details | `{"dependency_fqn": "schema.object", "dependency_type": "table\|view\|function", "error_code": "PARSE_ERROR"}` |

#### TRANSITIVE_SCOPE_LEAK

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A dependency has out-of-scope or missing references. |
| Current state | No transitive scope analysis. |
| User impact | Migration appears complete but dependency tree has gaps. |
| Remediation | Audit full dependency tree. |
| Details | `{"dependency_fqn": "schema.fn_b", "leaked_reference": "other_db.schema.table"}` |

### Dialect-Specific

#### PACKAGE_FUNCTION (Oracle)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Oracle |
| Trigger | The function is defined inside an Oracle PACKAGE. `_extract_definitions()` in `oracle_extract.py` queries `ALL_OBJECTS WHERE OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')` — standalone only. `_extract_proc_params()` filters `PACKAGE_NAME IS NULL`. Package functions are invisible to extraction. |
| Current state | Package functions are silently excluded. No catalog entry, no diagnostic. If a view or procedure references a package function, the reference appears as `MISSING_REFERENCE` with no indication the function exists inside a package. |
| User impact | The migration pipeline cannot analyze or migrate package functions. Any object depending on a package function has an unresolvable dependency. |
| Remediation | Extract package function DDL manually (`DBMS_METADATA.GET_DDL('PACKAGE_BODY', ...)`). Consider decomposing the package into standalone functions for migration. |
| Details | `{"package_name": "schema.package", "function_name": "func_name"}` |

#### PIPELINED_FUNCTION (Oracle)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Oracle |
| Trigger | The function DDL contains the `PIPELINED` keyword. Pipelined table functions use `PIPE ROW` and a collection return type — constructs that sqlglot's T-SQL/Oracle dialect may not structurally parse, producing a `Command` node or `DdlParseError`. |
| Current state | If sqlglot fails, the function gets `parse_error` set. The `PIPELINED` keyword is not specifically detected or flagged. |
| User impact | Pipelined functions have no dbt equivalent. The migration must replace them with a different pattern (e.g., a dbt model that materializes the function output). Without a specific diagnostic, the user only sees a generic `PARSE_ERROR`. |
| Remediation | Identify pipelined functions by scanning DDL for the `PIPELINED` keyword. Consider materializing the function output as a dbt model or creating a custom macro. |
| Details | `{"keyword": "PIPELINED", "return_type": "<collection type>"}` |

#### INVALID_SOURCE_OBJECT (Oracle)

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Oracle |
| Trigger | Oracle function has `STATUS != 'VALID'`. Excluded from extraction. |
| Current state | Silently excluded. |
| User impact | Invisible to migration. References appear as `MISSING_REFERENCE`. |
| Remediation | Fix compilation error at Oracle source. |
| Details | `{"oracle_status": "INVALID", "object_type": "FUNCTION"}` |

#### MULTI_TABLE_READ

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A function reads from N or more distinct tables (configurable threshold, suggested default: 5). Detected by counting entries in `references.tables.in_scope`. |
| Current state | No complexity signal exists for functions. |
| User impact | Functions with many table dependencies are complex to migrate. They may indicate a function that should be refactored into a view or a dbt model rather than being inlined as a macro. |
| Remediation | Review whether the function should be migrated as a macro (simple case) or materialized as a model (complex case). |
| Details | `{"table_count": 7, "tables": ["schema.t1", "schema.t2", "..."]}` |

#### SUBTYPE_UNKNOWN (SQL Server)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | SQL Server extracts functions with type codes `FN` (scalar), `IF` (inline table-valued), and `TF` (multi-statement table-valued) but stores all three in the same `functions` bucket with no subtype discrimination. |
| Current state | The function subtype is available in `object_types.json` but not propagated to the function catalog. |
| User impact | Migration strategy differs significantly by subtype: scalar functions may be inlined as dbt macros, inline TVFs can become CTEs or subqueries, and multi-statement TVFs need to be materialized. Without the subtype, the migration skill cannot choose the right strategy. |
| Remediation | During enrichment, look up the function's type code from `object_types.json` and write it to the catalog as a `subtype` field. |
| Details | `{"type_code": "FN\|IF\|TF"}` |

## Procedure Diagnostic Gaps

### Existing Diagnostic

#### MULTI_TABLE_WRITE (exists)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Procedure writes to more than one table (`is_updated=true` on >1 entry in `references.tables.in_scope`). |
| Current state | **Implemented** in `write_object_catalog()` at `catalog.py` lines 392-406. The only diagnostic that currently fires. |
| User impact | Cannot produce a single clean refactored SQL. The procedure must be split into multiple dbt models or handled with a custom macro. |
| Remediation | Split the procedure into one model per target table, or use a dbt `run_query` macro for multi-table operations. |
| Details | `{"tables": ["schema.table1", "schema.table2"]}` |

### Parse and Structure Gaps

#### PARSE_ERROR

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Both |
| Trigger | sqlglot cannot parse the procedure body. `ast=None`, `parse_error` is set on the `DdlEntry`. |
| Current state | **Gap.** `parse_error` string is stored on the `DdlEntry` and appears in `discover show` output, but is never written to the procedure catalog as a `warnings[]`/`errors[]` entry. `catalog_enrich.py` logs `event=enrich_skip reason=parse_error` and skips the procedure. |
| User impact | The procedure is silently skipped during enrichment. Its catalog has no AST-derived references — only DMF data. No diagnostic signals to the user that this procedure needs manual attention. |
| Remediation | Same as view `PARSE_ERROR`. |
| Details | `{"parse_error": "<message>"}` |

#### UNSUPPORTED_SYNTAX

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | sqlglot returns `exp.Command` nodes for subtrees within the procedure body. Also triggered when `classify_statement()` returns `needs_llm` for a non-EXEC `Command` node. |
| Current state | **Gap.** `unsupported_syntax` is not yet emitted by `scan_routing_flags()`. |
| User impact | References within opaque subtrees are missed. The procedure's `needs_llm` flag may be set, but the specific construct that triggered it is not identified. |
| Remediation | Review the specific construct. |
| Details | `{"command_text": "<raw text>", "statement_index": 3}` |

#### DUPLICATE_DEFINITION

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Same procedure FQN appears more than once in DDL file(s). |
| Current state | **Gap.** Silent overwrite. |
| User impact | Earlier definition lost. |
| Remediation | Deduplicate. |
| Details | `{"occurrences": 2}` |

#### STALE_OBJECT

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Procedure marked `stale: true` after re-extraction. |
| Current state | **Gap.** `stale` field set but no `warnings[]` entry. |
| User impact | Stale procedure remains in catalog, confusing dependency analysis. |
| Remediation | Re-run `setup-ddl`. |
| Details | `{"previous_ddl_hash": "<sha256>"}` |

#### SEGMENTER_LIMIT

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | The block segmenter in `block_segmenter.py` hits the max nesting depth (20) or max node count (500), raising `SegmenterLimitError`. The procedure is escalated to `needs_llm=true`. |
| Current state | **Gap.** Documented as `depth_limit_exceeded` routing reason in the design doc but not emitted as a distinct diagnostic. The procedure simply gets `needs_llm=true` with no explanation of why. |
| User impact | The user sees the procedure requires LLM assistance but doesn't know it's due to extreme nesting depth rather than dynamic SQL. The remediation path is different. |
| Remediation | Refactor the procedure to reduce nesting depth. Extract deeply nested branches into sub-procedures. |
| Details | `{"limit_type": "depth\|node_count", "value": 20, "max": 20}` |

#### GOTO_DETECTED

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | The procedure body contains a `GOTO` label. The block segmenter does not handle `GOTO` — it will produce a `SegmenterError` or incorrect parse. |
| Current state | **Gap.** `GOTO` is not in `_STATEMENT_STARTERS`. The segmenter fails, and the procedure is escalated to `needs_llm=true` via the generic `SegmenterError` path. |
| User impact | Same as `SEGMENTER_LIMIT` — the user doesn't know the root cause. `GOTO`-based procedures need fundamentally different refactoring (control flow restructuring) compared to deeply nested but structured procedures. |
| Remediation | Refactor the procedure to eliminate `GOTO` labels. Replace with structured control flow (`IF/ELSE`, `WHILE`, `BREAK`/`CONTINUE`). |
| Details | `{"label": "retry_label"}` |

### Reference Resolution Gaps

#### MISSING_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Procedure references a table/view/function not in the catalog. |
| Current state | **Gap.** No diagnostic. The reference is either in `out_of_scope` or simply absent. |
| User impact | Migration output references a missing object. |
| Remediation | Include the object in extraction scope. |
| Details | `{"missing_fqn": "schema.object", "reference_type": "table\|view\|function"}` |

#### OUT_OF_SCOPE_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | Cross-database or cross-server reference. |
| Current state | **Gap.** In `out_of_scope` but no `warnings[]` entry. |
| User impact | External dependency not in dbt project. |
| Remediation | Create dbt `source` or replace reference. |
| Details | `{"fqn": "server.db.schema.object", "reason": "cross_database_reference\|cross_server_reference"}` |

#### AMBIGUOUS_REFERENCE

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF `is_ambiguous=true`. |
| Current state | **Gap.** Stored but never surfaced. |
| User impact | Column lineage unreliable. |
| Remediation | Qualify column references. |
| Details | `{"reference_fqn": "schema.table"}` |

#### DMF_ERROR

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | `sys.dm_sql_referenced_entities` raised an exception for this procedure. `_run_dmf_queries()` in `sqlserver_extract.py` inserts an error sentinel row with `referenced_class_desc = "ERROR: <exc>"`. This sentinel falls through `_classify_referenced_type()` and lands in the `tables` bucket as a phantom entry. |
| Current state | **Gap.** The error is logged (`event=sqlserver_dmf_skip`) but no diagnostic is written to the catalog. The phantom table entry has no indication it's error-derived. |
| User impact | The procedure's `references.tables.in_scope` contains a bogus entry with an error message as the name. Dependency analysis is corrupted. |
| Remediation | Inspect the DMF error. Common causes: the procedure references a dropped object, has unresolved synonyms, or contains syntax errors that prevent DMF resolution. Fix the root cause and re-extract. |
| Details | `{"dmf_error": "<exception message>"}` |

#### DMF_MISCLASSIFIED

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | DMF `OBJECT_OR_COLUMN` defaulted to `tables` bucket. |
| Current state | **Gap.** Silent. |
| User impact | View/function reference appears as table. |
| Remediation | Cross-check against view/function catalogs. |
| Details | `{"misclassified_fqn": "schema.name", "assigned_bucket": "tables"}` |

#### CROSS_DB_EXEC

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | The procedure contains a 3-part `EXEC` call (e.g., `EXEC OtherDB.dbo.proc_name`). Detected by `_CROSS_DB_EXEC_RE` in `catalog.py`. Already a routing reason (`cross_db_exec`) but not a `warnings[]` entry. |
| Current state | **Gap.** `cross_db_exec` is set in `routing_reasons` and `needs_enrich=true` is set, but no `warnings[]` entry is created. |
| User impact | The called procedure is in another database and will not exist in the dbt project. The enrichment BFS cannot follow the call because the callee is not in the local catalog. |
| Remediation | Determine whether the cross-database call can be replaced with a local equivalent, a dbt `source` ref, or removed entirely. |
| Details | `{"exec_target": "OtherDB.dbo.proc_name", "parts": 3}` |

#### LINKED_SERVER_EXEC

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | SQL Server |
| Trigger | The procedure contains a 4-part `EXEC` call (e.g., `EXEC LinkedSrv.DB.dbo.proc_name`). Detected by `_LINKED_SERVER_EXEC_RE`. Already a routing reason (`linked_server_exec`). |
| Current state | **Gap.** Routing reason set but no `warnings[]` entry. |
| User impact | The called procedure is on a linked server. Cannot be resolved or migrated. |
| Remediation | Replace with a local equivalent or remove the linked server dependency. |
| Details | `{"exec_target": "Server.DB.dbo.proc_name", "parts": 4}` |

### Cross-Object and Transitive Gaps

#### CIRCULAR_REFERENCE

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Both |
| Trigger | Procedure A EXECs Procedure B, which EXECs Procedure A (cycle in the EXEC call graph). The BFS in `_build_writer_maps()` uses a `visited` set for cycle safety but does not emit a diagnostic when a cycle is detected. |
| Current state | **Gap.** The BFS silently skips already-visited nodes. No diagnostic is emitted. |
| User impact | Circular EXEC chains indicate either mutual recursion (intentional but hard to migrate) or a bug. The migration cannot produce a valid dbt DAG. |
| Remediation | Break the cycle by refactoring one procedure. If mutual recursion is intentional, document it and handle manually during migration. |
| Details | `{"cycle": ["schema.proc_a", "schema.proc_b", "schema.proc_a"]}` |

#### DEPENDENCY_HAS_ERROR

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A callee (via EXEC) or referenced view/function has an error-level diagnostic. |
| Current state | **Gap.** No transitive propagation. |
| User impact | Procedure may work in isolation but a dependency is broken. |
| Remediation | Fix the dependency's error first. |
| Details | `{"dependency_fqn": "schema.object", "error_code": "PARSE_ERROR"}` |

#### TRANSITIVE_SCOPE_LEAK

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Both |
| Trigger | A callee or referenced object has out-of-scope or missing references. |
| Current state | **Gap.** No transitive analysis. |
| User impact | Migration appears complete but the dependency tree has external gaps. |
| Remediation | Audit full dependency tree. |
| Details | `{"dependency_fqn": "schema.callee", "leaked_reference": "other_db.schema.table"}` |

### Dialect-Specific Gaps

#### PACKAGE_PROCEDURE (Oracle)

| Field | Value |
|---|---|
| Severity | warning |
| Dialects | Oracle |
| Trigger | Procedure defined inside an Oracle PACKAGE. Invisible to extraction. |
| Current state | **Gap.** Silently excluded. |
| User impact | Any object referencing a package procedure sees `MISSING_REFERENCE` with no root cause. |
| Remediation | Extract manually. Decompose the package. |
| Details | `{"package_name": "schema.package"}` |

#### INVALID_SOURCE_OBJECT (Oracle)

| Field | Value |
|---|---|
| Severity | error |
| Dialects | Oracle |
| Trigger | Oracle procedure has `STATUS != 'VALID'`. Excluded from extraction. |
| Current state | **Gap.** Silently excluded. |
| User impact | Invisible to migration. |
| Remediation | Fix compilation error at source. |
| Details | `{"oracle_status": "INVALID"}` |

## Summary Matrix

| Code | View | Function | Procedure | Severity | SQL Server | Oracle |
|---|---|---|---|---|---|---|
| `PARSE_ERROR` | Y | Y | Gap | error | Y | Y |
| `UNSUPPORTED_SYNTAX` | Y | Y | Gap | warning | Y | Y |
| `DUPLICATE_DEFINITION` | Y | Y | Gap | warning | Y | Y |
| `STALE_OBJECT` | Y | Y | Gap | warning | Y | Y |
| `MISSING_REFERENCE` | Y | Y | Gap | warning | Y | Y |
| `OUT_OF_SCOPE_REFERENCE` | Y | Y | Gap | warning | Y | Y |
| `AMBIGUOUS_REFERENCE` | Y | Y | Gap | warning | Y | - |
| `DMF_MISCLASSIFIED` | Y | Y | Gap | warning | Y | - |
| `CIRCULAR_REFERENCE` | Y | Y | Gap | error | Y | Y |
| `DEPENDENCY_HAS_ERROR` | Y | Y | Gap | warning | Y | Y |
| `TRANSITIVE_SCOPE_LEAK` | Y | Y | Gap | warning | Y | Y |
| `WRITER_THROUGH_VIEW` | Y | - | - | warning | Y | Y |
| `NESTED_VIEW_CHAIN` | Y | - | - | warning | Y | Y |
| `LONG_TRUNCATION` | Y | - | - | error | - | Y |
| `INVALID_SOURCE_OBJECT` | Y | Y | Gap | error | - | Y |
| `MATERIALIZED_VIEW_AS_TABLE` | Y | - | - | warning | - | Y |
| `PACKAGE_FUNCTION` | - | Y | - | warning | - | Y |
| `PACKAGE_PROCEDURE` | - | - | Gap | warning | - | Y |
| `PIPELINED_FUNCTION` | - | Y | - | warning | - | Y |
| `MULTI_TABLE_READ` | - | Y | - | warning | Y | Y |
| `SUBTYPE_UNKNOWN` | - | Y | - | warning | Y | - |
| `MULTI_TABLE_WRITE` | - | - | **Exists** | warning | Y | Y |
| `DMF_ERROR` | - | - | Gap | warning | Y | - |
| `SEGMENTER_LIMIT` | - | - | Gap | warning | Y | Y |
| `GOTO_DETECTED` | - | - | Gap | warning | Y | - |
| `CROSS_DB_EXEC` | - | - | Gap | warning | Y | - |
| `LINKED_SERVER_EXEC` | - | - | Gap | warning | Y | - |

**Totals:** 27 unique diagnostic codes. 14 apply to views, 14 to functions, 19 to procedures (1 existing + 18 gaps). 21 apply to SQL Server, 19 to Oracle.
