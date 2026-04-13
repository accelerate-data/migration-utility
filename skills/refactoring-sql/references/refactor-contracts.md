# Refactor Contracts

Use this reference for payload shape, semantic-review output, and command error mapping.

## Persisted Refactor Payload

`refactor write` persists a `refactor` section with these keys:

- `status`
- `extracted_sql`
- `refactored_sql`
- `semantic_review`
- `compare_sql` when executable compare ran
- `warnings`
- `errors`

Do not invent extra fields.

## Semantic Review Contract

The semantic-review sub-agent returns exactly one JSON object with:

```json
{
  "passed": true,
  "checks": {
    "source_tables": { "passed": true, "summary": "..." },
    "output_columns": { "passed": true, "summary": "..." },
    "joins": { "passed": true, "summary": "..." },
    "filters": { "passed": true, "summary": "..." },
    "aggregation_grain": { "passed": true, "summary": "..." }
  },
  "issues": [
    {
      "code": "EQUIVALENCE_PARTIAL",
      "message": "Refactored SQL drops the inactive-customer filter from the extracted SQL.",
      "severity": "warning"
    }
  ]
}
```

Rules:

- Compare extracted SQL to refactored SQL, not to dbt expectations.
- Use only these checks: source tables, output columns, joins, filters, aggregation grain.
- `issues[]` entries use diagnostics-style objects.
- If any check fails, `passed` must be `false`.

## Command Error Mapping

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Return `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` and mention the missing prerequisite |
| `refactor context` | 2 | IO/parse error. Return `status: "error"` with code `CONTEXT_IO_ERROR` and surface the error message |
| `refactor write` | 1 | Validation failure. Fix the payload and retry once |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | One or more scenarios failed. Enter the self-correction loop |
