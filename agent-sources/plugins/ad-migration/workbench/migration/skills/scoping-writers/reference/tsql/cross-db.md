# T-SQL Cross-Database Patterns

Cross-database references in T-SQL use three-part or four-part qualified names where the database component differs from the current context.

## Patterns to detect

| Pattern | Example |
|---|---|
| Bracketed three-part name | `[OtherDB].[dbo].[Employees]` |
| Unbracketed three-part name | `OtherDB.dbo.Employees` |
| Four-part linked-server name | `[LinkedServer].[OtherDB].[dbo].[Employees]` |

A name is a cross-database reference when the database component is present **and** differs from the source database being analysed.

## Action

If a cross-database reference is detected in any candidate procedure, apply both of the following:

- Set `status = "error"`
- Add error code `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE`
- Skip all remaining analysis steps for that item
