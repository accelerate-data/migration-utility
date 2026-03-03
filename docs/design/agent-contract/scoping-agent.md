# Scoping Agent Contract

The scoping agent maps a target table to one or more candidate writer procedures in SQL Server.
It is the prerequisite step for profiler input.

## Philosophy and Boundary

- Scoping is responsible only for writer discovery and writer selection.
- Scoping should not output data that profiler can derive reliably from the selected writer.
- Keep scoping payload minimal for clear handoff.

## Goal

Given a target table, identify candidate writer procedures and select one writer when resolvable.

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
        "allow_cross_database": true
      }
    }
  ]
}
```

## Discovery Strategy

1. Use SQL Server dependency metadata to find procedures that reference the target table.
2. Resolve indirect calls within configured depth.
3. Rank writer candidates by confidence.
4. Return selected writer when resolution is unambiguous.

## Output Schema

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "tables": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "resolved|ambiguous_multi_writer|no_writer_found|partial|error",
      "selected_writer": "dbo.usp_load_fact_sales",
      "candidate_writers": [
        {
          "procedure_name": "dbo.usp_load_fact_sales",
          "confidence": 0.98
        }
      ],
      "validation": {
        "passed": true,
        "issues": []
      },
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "resolved": 1,
    "ambiguous_multi_writer": 0,
    "no_writer_found": 0,
    "partial": 0,
    "error": 0
  }
}
```

## Resolution Rules

- `resolved`: exactly one high-confidence writer exists.
- `ambiguous_multi_writer`: multiple high-confidence writers exist.
- `partial`: incomplete writer evidence.
- `no_writer_found`: no writer candidate found.
- `error`: scoping execution failed for the table.

## Known Limitations

- Dynamic SQL (`sp_executesql`, `EXEC(@sql)`) may hide dependencies from metadata.
- Synonyms/views may mask base-table writers.
- Cross-database/server writes may require additional resolution.

## Handoff to Profiler

Profiler consumes only selected scoping fields.

```json
{
  "schema_version": "1.0",
  "batch_id": "uuid",
  "items": [
    {
      "item_id": "dbo.fact_sales",
      "target_table": "dbo.fact_sales",
      "status": "resolved",
      "selected_writer": "dbo.usp_load_fact_sales"
    }
  ]
}
```
