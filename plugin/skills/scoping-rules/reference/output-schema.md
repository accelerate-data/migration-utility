# Output Schema

Write only valid JSON to the output file. No markdown fences, no explanation.

```json
{
  "schema_version": "1.0",
  "run_id": "<run_id from input>",
  "results": [
    {
      "item_id": "schema.table",
      "status": "resolved",
      "selected_writer": "schema.proc_name",
      "candidate_writers": [
        {
          "procedure_name": "schema.proc_name",
          "write_type": "direct",
          "call_path": ["schema.proc_name"],
          "rationale": "Direct INSERT INTO target table found in procedure body.",
          "confidence": 0.90
        }
      ],
      "warnings": [],
      "validation": {"passed": true, "issues": []},
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

## Field notes

- Omit `selected_writer` entirely when status is not `resolved`
- `call_path` — ordered list from entry-point candidate to the procedure that performs the write
- `summary` counts must match item-level statuses exactly
