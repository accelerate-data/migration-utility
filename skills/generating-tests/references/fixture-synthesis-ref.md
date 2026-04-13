# Fixture Synthesis Reference

Rules for generating synthetic fixture rows in test scenarios. Prefer minimal valid data that still looks plausible for the branch being tested.

## Columns to exclude

Never include these columns in fixture rows — they will cause INSERT failures:

- **Computed columns**: Columns defined with `AS <expression>` in the DDL. Detect them from the `CREATE TABLE` statement in catalog DDL or from the proc context.
- **Identity columns not needed by the scenario**: Columns listed in `auto_increment_columns` where the scenario does not need to control the specific key value. Omit them and let the database auto-generate. Only include identity columns when the scenario requires a specific value (e.g., to set up a MERGE MATCHED condition with a known key).

## NOT NULL column coverage

Include **all** columns where `is_nullable == false` in the fixture rows, except computed columns and identity columns that the scenario does not need.

For columns that are NOT NULL but not referenced by the SQL logic, use these priorities:

1. A semantically plausible value consistent with the branch being tested
2. A value that satisfies any foreign key relationship within the same scenario
3. A simple type-appropriate fallback only when the catalog gives no better signal

Fallbacks should be valid and boring, not sentinel-heavy. Prefer ordinary examples such as:

- positive numeric values for counts, amounts, and identifiers
- short non-empty strings for required text
- realistic dates that satisfy ordering constraints
- booleans that match the branch you are trying to reach

Avoid placeholder patterns such as empty strings, zero GUIDs, and `1900-01-01` unless the scenario specifically needs them.

When a NOT NULL column also has a foreign key constraint, prefer a value backed by a row in the same scenario.

## CHECK constraint compliance

If the DDL or proc context reveals CHECK constraints on a source table, generate fixture values that satisfy them. Common patterns:

- Range constraints (`CHECK (Qty >= 0)`) — use a value within the range
- Enum constraints (`CHECK (Status IN ('A','B','C'))`) — pick a valid value
- Cross-column constraints (`CHECK (EndDate > StartDate)`) — ensure consistency

Do not generate values that violate CHECK constraints.

## Practical bias

- Prefer the smallest row set that reaches the branch.
- Prefer values that make the scenario easy to read.
- Prefer branch-specific fixtures over generic "one size fits all" rows.
