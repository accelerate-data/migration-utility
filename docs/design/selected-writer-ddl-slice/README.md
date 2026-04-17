# Selected Writer DDL Slice

## Decision

LLM-facing table contexts must expose only target-specific writer SQL when the selected writer is a sliced multi-table procedure.

When a selected writer has no `table_slices`, the normal `proc_body` field remains the full procedure DDL and `selected_writer_ddl_slice` is empty.

When a selected writer has `table_slices`, the requested table's slice is emitted as `selected_writer_ddl_slice` and `proc_body` is empty.

When a selected writer has `table_slices` but no slice for the requested table, context assembly fails and downstream LLM workflow stops.

## Rationale

Full multi-table procedure SQL is unsafe LLM context for table-specific profiling, refactoring, test generation, model generation, and model review because unrelated target-table logic can be treated as relevant business logic.

Keeping `proc_body` empty for sliced writers preserves its existing meaning: when present, it is the full procedure DDL. The selected slice gets its own field so agents can use a simple source order: `selected_writer_ddl_slice`, then `proc_body`.

## Scope Gate

`analyzing-table` owns deciding whether multi-table writer logic can be safely sliced.

If target-table logic is separable, the skill persists the table slice in the procedure catalog before selecting the writer.

If target-table logic is interleaved or cannot be attributed to one target table, the skill must not select the writer and must persist a `SCOPING_FAILED` scoping error.

Downstream readiness already blocks tables whose scoping status is not resolved. Context builders still enforce the slice invariant so stale or manually edited catalogs cannot leak full procedure SQL into LLM prompts.

## Affected Contexts

This contract applies from profile onward for table workflows:

- `profile context`
- `refactor context`
- `migrate context`
- skills that consume those contexts through profiling, refactoring, test generation, model generation, and model review

View and materialized-view contexts remain separate because they use `view_sql`, not writer procedure SQL.
