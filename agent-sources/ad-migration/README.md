# ad-migration — Claude Code Plugin

Analyse Microsoft SQL Server and Fabric Warehouse stored procedures and extract DDL structure for migration to dbt on Vibedata Managed Fabric Lakehouse.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — Python package manager (required for all Python skills)
- [genai-toolbox](https://github.com/googleapis/genai-toolbox) `toolbox` binary on PATH — required only for the `setup-ddl` skill (live SQL Server connection)
- Microsoft SQL Server accessible via network — required only for `setup-ddl`

## Skills

| Skill | Invocable | Trigger |
|---|---|---|
| `discover` | No (reference) | Loaded when exploring a DDL directory |
| `scope` | No (reference) | Loaded when finding writer procedures |
| `setup-ddl` | Yes | "set up DDL", "extract DDL from SQL Server", "populate artifacts/ddl" |

## Agents

| Agent | Purpose |
|---|---|
| `scoping-agent` | Given a target table, identify which procedure writes to it |

## MCP Servers

| Server | Purpose | Requires |
|---|---|---|
| `ddl` | Structured DDL parsing from local files | uv |
| `mssql` | Live SQL Server query execution | `toolbox` binary, env vars |

## Environment Variables (setup-ddl only)

| Variable | Description |
|---|---|
| `MSSQL_HOST` | SQL Server hostname or IP |
| `MSSQL_PORT` | SQL Server port (default: 1433) |
| `MSSQL_DB` | Database name |
| `SA_PASSWORD` | SQL Server password |

## Tools vs MCP Decision Rule

| Access type | Use |
|---|---|
| Plain file I/O (read a .sql file as text) | Native `Read`/`Write`/`Glob` tools |
| Structured DDL parsing (columns, AST dependencies) | `ddl` MCP (`ddl:list_tables`, `ddl:get_procedure_body`, etc.) |
| Remote SQL Server queries | `mssql` MCP (`mssql:mssql-execute-sql`) |

## Loading Plugins

To run Claude Code with all ad-migration plugins loaded:

```bash
claude --plugin-dir ./agent-sources/ad-migration/workbench/bootstrap \
       --plugin-dir ./agent-sources/ad-migration/workbench/migration \
       --plugin-dir ./agent-sources/ad-migration/workbench/test-generation
```

## Running Skills Manually

```bash
# List all tables in a DDL directory
uv run --project agent-sources/ad-migration/workbench/migration/shared discover \
  --ddl-path ./artifacts/ddl --type tables

# Find writer procedures for a target table
uv run --project agent-sources/ad-migration/workbench/migration/shared scope \
  --ddl-path ./artifacts/ddl --table dbo.FactSales
```

## License

Elastic License 2.0 — see [LICENSE](LICENSE).
