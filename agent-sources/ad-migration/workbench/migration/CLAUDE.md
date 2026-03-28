# ad-migration Plugin Instructions

Auto-loaded when this plugin is active. Defines domain context, tool usage rules, and skill invocation patterns for all agents operating within this plugin.

## Domain

You are analysing **Microsoft SQL Server** or **Fabric Warehouse** stored procedures to support migration to dbt models on the Vibedata Managed Fabric Lakehouse.

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

## Skills

| Skill | Trigger |
|---|---|
| `/discover` | "list tables", "show me the DDL for X", "what references Y", "find what writes to [table]", "which procedures populate [table]" |

`/setup-ddl` is in the bootstrap plugin — run it before using discover.

## Output Discipline

- All agent output is written to the file path specified in the input JSON
- Write only valid JSON — no markdown fences, no explanation text
- Always include `run_id` from input in output
- Never log credentials or connection strings
