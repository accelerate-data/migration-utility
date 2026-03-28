# ad-migration Plugin Instructions

Auto-loaded when this plugin is active. Defines domain context, tool usage rules,
and skill invocation patterns for all agents operating within this plugin.

## Domain

You are analysing **Microsoft SQL Server** or **Fabric Warehouse** stored procedures
to support migration to dbt models on the Vibedata Managed Fabric Lakehouse.

Source objects: T-SQL stored procedures, table DDL, views, functions.
Migration target: silver and gold dbt transformations only. Bronze, ADF pipelines,
and Power BI are out of scope.

## Tools vs MCP — Decision Rule

| Access type | Use | Why |
|---|---|---|
| Plain file read/write (`.sql`, `.json`, `.md`) | Native `Read`/`Write`/`Glob` tools | No overhead; direct filesystem access |
| Structured DDL parsing (columns, AST dependencies, normalized names) | `ddl` MCP server | Parsing is done by sqlglot — not available from raw file reads |
| Remote SQL Server queries | `mssql` MCP server | Credentials managed by genai-toolbox; no ODBC driver needed |

Never use `ddl` MCP for plain file reads. Never use native Read tool to parse DDL structure.

## Skill Invocation

### discover (reference skill — do not invoke directly)

Loaded automatically when exploring a DDL directory. Provides instructions for
using the `discover` CLI to list, inspect, and trace references between DDL objects.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" discover list \
  --ddl-path <path> --type tables
```

### scope (reference skill — do not invoke directly)

Loaded automatically when identifying writer procedures. Provides instructions for
using the `scope` CLI to find which procedures write to a target table.

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" scope \
  --ddl-path <path> --table dbo.FactSales
```

### setup-ddl (user-invocable)

Triggered when the user asks to populate `artifacts/ddl/` from a live SQL Server.
Uses `mssql` MCP for queries and native Write tool for local file output.

## Output Discipline

- All agent output is written to the file path specified in the input JSON
- Write only valid JSON — no markdown fences, no explanation text
- Always include `run_id` from input in output
- Never log credentials or connection strings
