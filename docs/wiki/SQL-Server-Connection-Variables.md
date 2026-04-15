# SQL Server Connection Variables

Use these variables when the configured technology is SQL Server.

`/init-ad-migration` scaffolds the shared non-secret values in `.envrc`. Keep secrets in `.env` or export them in your shell before running commands.

## Source runtime

Required by `ad-migration setup-source`.

| Variable | Description |
|---|---|
| `SOURCE_MSSQL_HOST` | Source SQL Server hostname or IP |
| `SOURCE_MSSQL_PORT` | Source SQL Server port |
| `SOURCE_MSSQL_DB` | Source database name |
| `SOURCE_MSSQL_USER` | Source SQL Server username |
| `SOURCE_MSSQL_PASSWORD` | Source SQL Server password |

## Target runtime

Required by `ad-migration setup-target` and downstream dbt validation.

| Variable | Description |
|---|---|
| `TARGET_MSSQL_HOST` | Target SQL Server hostname or IP |
| `TARGET_MSSQL_PORT` | Target SQL Server port |
| `TARGET_MSSQL_DB` | Target database name |
| `TARGET_MSSQL_USER` | Target SQL Server username |
| `TARGET_MSSQL_PASSWORD` | Target SQL Server password |

## Sandbox runtime

Required by `ad-migration setup-sandbox`.

| Variable | Description |
|---|---|
| `SANDBOX_MSSQL_HOST` | Sandbox SQL Server hostname or IP |
| `SANDBOX_MSSQL_PORT` | Sandbox SQL Server port |
| `SANDBOX_MSSQL_USER` | Sandbox SQL Server username |
| `SANDBOX_MSSQL_PASSWORD` | Sandbox SQL Server password |

## Optional driver override

| Variable | Description |
|---|---|
| `MSSQL_DRIVER` | Optional ODBC driver override. Defaults to `FreeTDS` when unset. |

Use `MSSQL_DRIVER=ODBC Driver 18 for SQL Server` only if that driver is installed and configured locally.

## Related pages

- [[Installation and Prerequisites]]
- [[CLI Reference]]
- [[Sandbox Operations]]
