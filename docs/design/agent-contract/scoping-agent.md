# Scoping Agent Contract

The scoping agent maps a target table to one or more candidate writer procedures in SQL Server.
It is the prerequisite step before profiler and planner.

## Goal

Given a target table, identify which stored procedure(s) write to it and return ranked candidates
with evidence. Support multi-writer scenarios.

## Required Input

```json
{
  "target_table": "dbo.fact_sales",
  "search_depth": 2,
  "constraints": {
    "include_orchestrators": true,
    "allow_cross_database": true
  }
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
  "request": {
    "target_table": "dbo.fact_sales",
    "search_depth": 2
  },
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
  "summary": {
    "high_confidence_writers": 1,
    "dynamic_sql_detected": false,
    "notes": []
  },
  "fde_action_required": {
    "required": false,
    "reason": "",
    "options": []
  }
}
```

## Resolution Rules

- Auto-resolve only when exactly one high-confidence direct writer exists.
- If multiple high-confidence direct writers exist, return `ambiguous_multi_writer` and require FDE selection.
- If only indirect writers are found, return `partial` and require FDE confirmation.
- If no writers are found, return `no_writer_found`.

## SQL Server Signals

- Dependency catalog: `sys.sql_expression_dependencies`, `sys.objects`
- Procedure text: `sys.sql_modules.definition`
- Procedure signature: `sys.procedures`, `sys.parameters`
- Additional dependency fallback: `sys.dm_sql_referenced_entities`

## Known Limitations

- Dynamic SQL (`sp_executesql`, `EXEC(@sql)`) may hide dependencies from metadata.
- Synonyms/views may mask base-table writers.
- Cross-database/server writes may require additional resolution.
