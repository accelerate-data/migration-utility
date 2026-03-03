# Scoping Agent Contract

The scoping agent maps a target table to one or more candidate writer procedures in SQL Server.
It is the prerequisite step before profiler and planner.

## Goal

Given a target table, identify which stored procedure(s) write to it and return ranked candidates
with evidence. Support multi-writer scenarios.

## Required Input

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "search_depth": 2,
      "constraints": {
        "include_orchestrators": true,
        "allow_cross_database": true,
        "scope_phase": "draft|finalized"
      },
      "user_context": {
        "selected_for_migration": true
      }
    }
  ]
}
```

## Discovery Strategy

1. Use SQL Server dependency metadata to find procedure candidates that reference target table.
2. Read candidate procedure SQL and classify write intent (`insert`, `update`, `delete`, `merge`, `truncate`).
3. Build procedure call graph to detect indirect writers (`EXEC proc_x`) within configured depth.
4. Rank candidates by confidence and return all confirmed writers.

## Output Schema

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "results": [
    {
      "item_id": "dbo.fact_sales",
      "status": "ok|partial|error",
      "request": {
        "target_table": "dbo.fact_sales",
        "search_depth": 2
      },
      "output": {
        "status": "resolved|ambiguous_multi_writer|no_writer_found|partial",
        "auto_selected_procedure": "dbo.usp_load_fact_sales",
        "candidates": [
          {
            "procedure_name": "dbo.usp_load_fact_sales",
            "write_type": "direct|indirect|read_only",
            "operations": ["insert", "merge", "update", "delete", "truncate", "exec_chain"],
            "confidence": 0.98,
            "evidence": {
              "dependency_source": "sys.sql_expression_dependencies|code_parse",
              "code_snippets": [
                "INSERT INTO dbo.fact_sales (...)"
              ],
              "call_path": ["dbo.usp_load_fact_sales"]
            },
            "risk_flags": [
              "dynamic_sql_detected",
              "cross_db_write",
              "manual_backfill_possible"
            ]
          }
        ],
        "provenance": {
          "writer_selection_source": "agent|manual",
          "manual_override": false,
          "analysis_confidence": 0.98,
          "analysis_rationale": [
            "Direct INSERT/TRUNCATE against target table found in procedure body."
          ]
        },
        "validation": {
          "passed": true,
          "issues": []
        }
      },
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "ok": 1,
    "partial": 0,
    "error": 0
  }
}
```

## Resolution Rules

- Auto-resolve only when exactly one high-confidence direct writer exists.
- If multiple high-confidence direct writers exist, return `ambiguous_multi_writer` and require FDE selection.
- If only indirect writers are found, return `partial` and require FDE confirmation.
- If no writers are found, return `no_writer_found`.
- Scoping can remain editable in `draft`; once `scope_phase` is `finalized`, scoping output is read-only.

## SQL Server Signals

- Dependency catalog: `sys.sql_expression_dependencies`, `sys.objects`
- Procedure text: `sys.sql_modules.definition`
- Procedure signature: `sys.procedures`, `sys.parameters`
- Additional dependency fallback: `sys.dm_sql_referenced_entities`

## Known Limitations

- Dynamic SQL (`sp_executesql`, `EXEC(@sql)`) may hide dependencies from metadata.
- Synonyms/views may mask base-table writers.
- Cross-database/server writes may require additional resolution.

## Handoff to Profiler

Scoping output feeds profiler input after FDE confirmation of writer selection.

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "procedure": { "name": "dbo.usp_load_fact_sales" },
      "target": {
        "name": "dbo.fact_sales",
        "intended_kind": "auto|dim_non_scd|dim_scd1|dim_scd2|dim_junk|fact_transaction|fact_periodic_snapshot|fact_accumulating_snapshot|fact_aggregate"
      },
      "constraints": {
        "business_context": "",
        "fde_overrides": []
      },
      "scope_context": {
        "scoping_status": "resolved",
        "selected_by": "agent|manual",
        "candidate_count": 1
      }
    }
  ]
}
```
