# Branch Patterns

Conditional branch enumeration patterns for test generation and review. Both the test generator and test reviewer use these tables to identify code paths that produce different output behavior.

## Table patterns (source routines)

| Pattern | Branches to enumerate |
|---|---|
| MERGE WHEN clauses | One per WHEN MATCHED, WHEN NOT MATCHED, WHEN NOT MATCHED BY SOURCE |
| CASE/WHEN | One per arm + ELSE |
| JOIN | Match, no-match (NULL right side for LEFT JOIN), partial multi-condition match |
| WHERE | Row that passes, row that fails |
| Subquery | EXISTS true/false, IN match/miss, correlated hit/miss |
| NULL handling | Nullable columns in filters/joins/COALESCE — NULL vs non-NULL |
| Aggregation | Single group, multiple groups, empty group |
| Type boundaries | Watermark date edges, MAX int, empty string |
| Empty source | Zero-row edge case per source table |

## View patterns (SELECT-level only)

| Pattern | Branches to enumerate |
|---|---|
| WHERE | Row that passes, row that fails |
| JOIN | Match, no-match (NULL right side for LEFT JOIN), partial multi-condition match |
| CASE/WHEN | One per arm + ELSE |
| Subquery | EXISTS true/false, IN match/miss, correlated hit/miss |
| NULL handling | Nullable columns in filters/joins/COALESCE — NULL vs non-NULL |
| Aggregation | Single group, multiple groups, empty group (with/without HAVING) |
| Empty source | Zero-row edge case per source table |
