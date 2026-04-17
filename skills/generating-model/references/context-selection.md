# Context Selection

Choose the SQL source before drafting dbt SQL.

## Source of Truth

| Context field | Use when | Rule |
|---|---|---|
| `selected_writer_ddl_slice` | Present | Use it for multi-table writers. |
| `refactored_sql` | No `selected_writer_ddl_slice` | Use it for ordinary writers and views. |
| `proc_body` | Never for generation | Review context only; do not generate from it. |

Preserve joins, filters, projections, grouping, and write intent from the
selected transformed SQL.

## Equivalence Pass

Compare the generated model against the selected transformed SQL before
returning:

- source tables
- selected columns
- joins and filters
- aggregation grain
- write semantics

If a semantic gap remains, add `EQUIVALENCE_GAP` to `warnings[]`.

## Common Mistakes

- Using full `proc_body` because the selected SQL looks incomplete.
- Ignoring `selected_writer_ddl_slice` and migrating unrelated target-table writes from a
  multi-table procedure.
- Running equivalence against `refactored_sql` when a table-specific
  `selected_writer_ddl_slice` is present.
