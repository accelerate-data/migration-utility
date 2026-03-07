# T-SQL DiscoverCandidates

## Check if target is a view

Call `list_views` to see if `item_id` is a view rather than a base table. If it is a view, perform all of the following:

- Call `get_view_body` to read its definition.
- Determine the underlying base table it reads from.
- Run `get_dependencies` on both the view name and the base table name.
- Note in warnings that the target is a view and show the base table.

## Find candidate procedures

Call `get_dependencies(table_name: <item_id>)`. This returns all procedures whose bodies reference the target table. These are the initial candidate set.

## Cross-database reference check

For each candidate procedure, call `get_procedure_body` and scan for cross-database references using the patterns in [cross-db.md](cross-db.md).

If any candidate procedure contains a cross-database reference, apply both of the following:

- Set `status = "error"` with error code `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE`
- Skip the remaining steps for that item.

## Empty result handling

If `get_dependencies` returns `(none)`, try `list_procedures` and spot-check bodies via `get_procedure_body` for any that might write to the target via dynamic SQL or indirect paths. Record a warning if none are found.
