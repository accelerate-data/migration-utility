# Input Schema

```json
{
  "schema_version": "1.0",
  "run_id": "<uuid>",
  "items": [
    {
      "item_id": "<schema>.<table>",
      "search_depth": 2
    }
  ]
}
```

## Field semantics

- `item_id` — schema-qualified target table or view name
- `search_depth` — maximum call-graph traversal depth (integer `0..5`, default `2`)
  - `0` = candidate procedure bodies only, no callee traversal
  - `1` = direct callees of candidates
  - `2+` = recursive up to N hops
