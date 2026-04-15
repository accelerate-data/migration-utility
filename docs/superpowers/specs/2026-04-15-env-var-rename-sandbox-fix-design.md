# Env Var Rename + Sandbox Cold-Start Fix

## Goal

Rename all connection env vars to a consistent `<ROLE>_<TECH>_<FIELD>` convention, remove unsupported target technologies (Fabric, Snowflake, DuckDB), and fix the sandbox cold-start gap so `setup-sandbox` reads env vars and writes `runtime.sandbox.connection` to `manifest.json` before connecting — matching the pattern already established by `setup-source`.

## Problem

Three issues exist today:

1. **Inconsistent naming**: source vars use technology-specific prefixes (`MSSQL_HOST`, `SA_PASSWORD`, `ORACLE_HOST`) with no role context. Target vars use a generic `TARGET_*` prefix with no technology disambiguation. There is no sandbox prefix at all.
2. **Sandbox cold-start**: `setup-sandbox` calls `SandboxBackend.from_env(manifest)` which reads `runtime.sandbox.connection` from `manifest.json`, but nothing ever writes those fields. The command fails with a confusing internal error rather than a clear env var message.
3. **Dead target technologies**: `setup-target` accepts `fabric`, `snowflake`, and `duckdb` but none have real backends. Only SQL Server and Oracle have `dbops/` implementations.

## Design

### Env Var Convention

All connection env vars follow `<ROLE>_<TECH>_<FIELD>`. Old names are deleted — no aliases.

**Source**

| Technology | Var | Description |
|---|---|---|
| sql_server | `SOURCE_MSSQL_HOST` | SQL Server hostname |
| sql_server | `SOURCE_MSSQL_PORT` | SQL Server port |
| sql_server | `SOURCE_MSSQL_DB` | Source database name |
| sql_server | `SOURCE_MSSQL_USER` | SQL Server username |
| sql_server | `SOURCE_MSSQL_PASSWORD` | SQL Server password |
| oracle | `SOURCE_ORACLE_HOST` | Oracle hostname |
| oracle | `SOURCE_ORACLE_PORT` | Oracle port |
| oracle | `SOURCE_ORACLE_SERVICE` | Oracle service name |
| oracle | `SOURCE_ORACLE_USER` | Oracle username |
| oracle | `SOURCE_ORACLE_PASSWORD` | Oracle password |

**Sandbox**

| Technology | Var | Description |
|---|---|---|
| sql_server | `SANDBOX_MSSQL_HOST` | Sandbox SQL Server hostname |
| sql_server | `SANDBOX_MSSQL_PORT` | Sandbox SQL Server port |
| sql_server | `SANDBOX_MSSQL_USER` | Sandbox SQL Server username |
| sql_server | `SANDBOX_MSSQL_PASSWORD` | Sandbox SQL Server password |
| oracle | `SANDBOX_ORACLE_HOST` | Sandbox Oracle hostname |
| oracle | `SANDBOX_ORACLE_PORT` | Sandbox Oracle port |
| oracle | `SANDBOX_ORACLE_SERVICE` | Sandbox Oracle service name |
| oracle | `SANDBOX_ORACLE_USER` | Sandbox Oracle admin username |
| oracle | `SANDBOX_ORACLE_PASSWORD` | Sandbox Oracle admin password |

**Target**

| Technology | Var | Description |
|---|---|---|
| sql_server | `TARGET_MSSQL_HOST` | Target SQL Server hostname |
| sql_server | `TARGET_MSSQL_PORT` | Target SQL Server port |
| sql_server | `TARGET_MSSQL_DB` | Target database name |
| sql_server | `TARGET_MSSQL_USER` | Target SQL Server username |
| sql_server | `TARGET_MSSQL_PASSWORD` | Target SQL Server password |
| oracle | `TARGET_ORACLE_HOST` | Target Oracle hostname |
| oracle | `TARGET_ORACLE_PORT` | Target Oracle port |
| oracle | `TARGET_ORACLE_SERVICE` | Target Oracle service name |
| oracle | `TARGET_ORACLE_USER` | Target Oracle username |
| oracle | `TARGET_ORACLE_PASSWORD` | Target Oracle password |

### Sandbox Command Flow (new)

`setup-sandbox` gains three steps before connecting:

1. Read `runtime.sandbox.technology` from manifest (seeded by init)
2. Call `require_sandbox_vars(technology)` — exits 1 with a clear message listing every missing var
3. Call `_write_sandbox_connection_to_manifest(root, manifest, technology)` — reads `SANDBOX_MSSQL_*` or `SANDBOX_ORACLE_*`, writes `runtime.sandbox.connection` fields into `manifest.json`
4. Proceed with `_create_backend(manifest)` as before

### Target Technology Scope

`setup-target` is narrowed to `sql_server` and `oracle`. Fabric, Snowflake, and DuckDB are removed from `_TARGET_ENV_MAPS`, `env_check.py`, and the command help text. No migration path — clean delete.

## Files Changed

| File | Change |
|---|---|
| `lib/shared/cli/env_check.py` | Rename `_SOURCE_VARS` entries; rename `_TARGET_VARS` to sql_server + oracle; add `_SANDBOX_VARS` dict + `require_sandbox_vars()` |
| `lib/shared/setup_ddl_support/manifest.py` | `get_connection_identity()` reads `SOURCE_MSSQL_*` / `SOURCE_ORACLE_*` |
| `lib/shared/target_setup.py` | Replace `_TARGET_ENV_MAPS` with sql_server + oracle entries; remove Fabric/Snowflake/DuckDB |
| `lib/shared/cli/setup_target_cmd.py` | Help text: `sql_server or oracle` |
| `lib/shared/sandbox/sql_server.py` | `from_env()` reads `SANDBOX_MSSQL_*`; `password_env` hardcoded to `"SANDBOX_MSSQL_PASSWORD"` |
| `lib/shared/sandbox/oracle.py` | `from_env()` reads `SANDBOX_ORACLE_*`; `password_env` hardcoded to `"SANDBOX_ORACLE_PASSWORD"` |
| `lib/shared/cli/setup_sandbox_cmd.py` | Add `_get_sandbox_technology()`, `require_sandbox_vars()` call, `_write_sandbox_connection_to_manifest()` |
| `lib/shared/init_templates.py` | `.envrc` template updated with new var names |
| `commands/init-ad-migration.md` | Env var tables updated for all three roles; target section narrowed |
| `docs/wiki/CLI-Reference.md` | Full env var reference updated |
| `tests/unit/cli/test_env_check.py` | Updated for new var names; sandbox tests added |
| `tests/unit/cli/test_sandbox_cmds.py` | Updated patches and assertions |
| `tests/unit/cli/test_setup_source_cmd.py` | Updated env var patches |
| `tests/unit/cli/test_setup_target_cmd.py` | Fabric/Snowflake/DuckDB tests removed; sql_server + oracle tests updated |

## Error Behaviour

Missing vars produce the same format already used by `setup-source`:

```text
Error: missing required environment variables for sql_server sandbox:

  SANDBOX_MSSQL_HOST      not set
  SANDBOX_MSSQL_PASSWORD  not set

Set these in your shell or .envrc before running setup-sandbox.
```

## Out of Scope

- `setup-target` backend implementation (dbt scaffolding only, no live connection)
- `teardown-sandbox` — reads sandbox name from manifest, no new env vars needed
- Oracle DSN-based connections — not changing that path
