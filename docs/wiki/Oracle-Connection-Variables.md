# Oracle Connection Variables

Use these variables when the configured technology is Oracle.

`/init-ad-migration` scaffolds the shared non-secret values in `.envrc`. Keep secrets in `.env` or export them in your shell before running commands.

## Source runtime

Required by `ad-migration setup-source`.

| Variable | Description |
|---|---|
| `SOURCE_ORACLE_HOST` | Source Oracle hostname or IP |
| `SOURCE_ORACLE_PORT` | Source Oracle port |
| `SOURCE_ORACLE_SERVICE` | Source Oracle service name |
| `SOURCE_ORACLE_SCHEMA` | Source Oracle schema/user selected for extraction |
| `SOURCE_ORACLE_USER` | Source Oracle username |
| `SOURCE_ORACLE_PASSWORD` | Source Oracle password |

## Target runtime

Required by `ad-migration setup-target` and downstream dbt validation.

| Variable | Description |
|---|---|
| `TARGET_ORACLE_HOST` | Target Oracle hostname or IP |
| `TARGET_ORACLE_PORT` | Target Oracle port |
| `TARGET_ORACLE_SERVICE` | Target Oracle service name |
| `TARGET_ORACLE_USER` | Target Oracle username |
| `TARGET_ORACLE_PASSWORD` | Target Oracle password |

## Sandbox runtime

Required by `ad-migration setup-sandbox`.

| Variable | Description |
|---|---|
| `SANDBOX_ORACLE_HOST` | Sandbox Oracle hostname or IP |
| `SANDBOX_ORACLE_PORT` | Sandbox Oracle port |
| `SANDBOX_ORACLE_SERVICE` | Sandbox Oracle service name |
| `SANDBOX_ORACLE_USER` | Sandbox Oracle admin username |
| `SANDBOX_ORACLE_PASSWORD` | Sandbox Oracle admin password |

## Related pages

- [[Installation and Prerequisites]]
- [[CLI Reference]]
- [[Sandbox Operations]]
