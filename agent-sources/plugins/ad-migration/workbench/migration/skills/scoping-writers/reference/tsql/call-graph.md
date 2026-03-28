# T-SQL Call Graph Patterns

## Call syntax to detect

| Pattern | Notes |
|---|---|
| `EXEC schema.procedure_name` | Direct call |
| `EXECUTE schema.procedure_name` | Direct call |
| `EXEC [schema].[procedure_name]` | Bracketed form |
| `EXEC @variable` | Dynamic call — cannot resolve statically; record as warning |
| `EXECUTE sp_executesql @stmt` | Dynamic SQL — cannot resolve call target statically |

Parse procedure body for all of the above patterns to build the callee list.

## Call path format

Record an ordered list from the entry-point candidate to the procedure that performs the write:

- Direct candidate: `["schema.proc"]`
- Callee at depth 1: `["schema.parent", "schema.callee"]`
- Callee at depth 2: `["schema.outer", "schema.middle", "schema.inner"]`
