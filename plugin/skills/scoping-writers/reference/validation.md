# Validation Checklist

Set `validation.passed = false` and add a diagnostics entry to `validation.issues[]` on any failure:

- `item_id` is present
- `status` is one of: `resolved`, `ambiguous_multi_writer`, `partial`, `no_writer_found`, `error`
- Every `confidence` is in [0.0, 1.0]
- Every candidate has `write_type`, `call_path`, and `rationale`
- `resolved` → `selected_writer` present and matches a `procedure_name` in `candidate_writers`
- `ambiguous_multi_writer` → ≥2 candidates with confidence > 0.7, no `selected_writer`
- `partial` → `candidate_writers` non-empty
- `no_writer_found` → `candidate_writers` empty, no `selected_writer`
- `error` → `errors` non-empty
- `summary` counts match item-level statuses exactly
