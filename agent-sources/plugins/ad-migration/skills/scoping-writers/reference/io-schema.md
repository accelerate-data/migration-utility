# I/O Schemas

## Input Schema

```json
{
  "schema_version": "1.0",
  "run_id": "<uuid>",
  "technology": "sql_server",
  "ddl_path": "/absolute/path/to/artifacts/ddl",
  "items": [
    {
      "item_id": "<schema>.<table>",
      "search_depth": 2
    }
  ]
}
```

### Field semantics

- `technology` — source technology; determines which skill patterns to apply. Values: `sql_server`, `fabric_warehouse`, `fabric_lakehouse`, `snowflake`
- `ddl_path` — absolute path to the DDL artifacts directory; passed to every MCP tool call so the server does not need a `DDL_PATH` environment variable
- `item_id` — schema-qualified target table or view name
- `search_depth` — maximum call-graph traversal depth (integer `0..5`, default `2`)
  - `0` = candidate procedure bodies only, no callee traversal
  - `1` = direct callees of candidates
  - `2+` = recursive up to N hops

---

## Output Schema

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

### Field notes

- Omit `selected_writer` entirely when status is not `resolved`
- `call_path` — ordered list from entry-point candidate to the procedure that performs the write
- `summary` counts must match item-level statuses exactly

---

## Diagnostics Format

All entries in `warnings[]`, `errors[]`, and `validation.issues[]` use this structure:

```json
{
  "code": "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE",
  "message": "Procedure references an out-of-scope database.",
  "field": "candidate_writers[0].procedure_name",
  "severity": "error",
  "details": {}
}
```

### Field notes

- `code` — stable machine-readable identifier
- `message` — human-readable description
- `field` — optional dot/bracket path to the affected field; omit for non-field errors
- `severity` — `error` or `warning`
- `details` — optional structured context; omit when empty

### Known error codes

| Code | Severity | Meaning |
|---|---|---|
| `ANALYSIS_UNSUPPORTED_TECHNOLOGY` | `error` | `technology` field is absent or not supported by this agent |
| `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE` | `error` | Candidate procedure references an out-of-scope database |
