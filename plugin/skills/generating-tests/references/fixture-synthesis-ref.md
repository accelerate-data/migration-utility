# Fixture Synthesis Reference

Rules for generating synthetic fixture rows in test scenarios. Apply these during Step 3 of the generating-tests skill.

## Columns to exclude

Never include these columns in fixture rows — they will cause INSERT failures:

- **Computed columns**: Columns defined with `AS <expression>` in the DDL. Detect them from the `CREATE TABLE` statement in catalog DDL or from the proc context. SQL Server rejects explicit values for computed columns.
- **Identity columns not needed by the scenario**: Columns listed in `auto_increment_columns` where the scenario does not need to control the specific key value. Omit them and let SQL Server auto-generate. Only include identity columns when the scenario requires a specific value (e.g., to set up a MERGE MATCHED condition with a known key).

## NOT NULL column coverage

Include **all** columns where `is_nullable == false` in the fixture rows — except computed columns and identity columns that the scenario does not need.

For columns that are NOT NULL but not referenced by the procedure SQL, use sensible type-appropriate defaults:

| SQL Type Pattern | Default Value |
|---|---|
| INT, BIGINT, SMALLINT, TINYINT | `0` |
| NVARCHAR, VARCHAR, CHAR, NCHAR | `""` (empty string) |
| DATETIME, DATETIME2, DATE, SMALLDATETIME | `"1900-01-01"` |
| BIT | `0` |
| DECIMAL, NUMERIC, MONEY, SMALLMONEY | `0.00` |
| FLOAT, REAL | `0.0` |
| UNIQUEIDENTIFIER | `"00000000-0000-0000-0000-000000000000"` |
| VARBINARY, BINARY | `""` (empty string) |

When a NOT NULL column also has a foreign key constraint, prefer a value that matches a row in the referenced table within the same scenario. If the referenced table is not part of the scenario fixtures, use the type default above — the sandbox disables FK constraints during fixture insertion so orphaned FK values will not cause failures.

## CHECK constraint compliance

If the DDL or proc context reveals CHECK constraints on a source table, generate fixture values that satisfy them. Common patterns:

- Range constraints (`CHECK (Qty >= 0)`) — use a value within the range
- Enum constraints (`CHECK (Status IN ('A','B','C'))`) — pick a valid value
- Cross-column constraints (`CHECK (EndDate > StartDate)`) — ensure consistency

Do not generate values that violate CHECK constraints — the sandbox does not disable CHECK constraints because violations indicate wrong fixture data.
