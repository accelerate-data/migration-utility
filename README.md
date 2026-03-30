# Migration Utility

A Claude Code plugin that migrates Microsoft Fabric Warehouse stored procedures to dbt models. Run it locally against your SQL Server — it extracts DDL, scopes which procedures write to each target table, profiles the tables, and generates dbt models with tests.

**Scope:** Silver and gold transformations from Fabric Warehouse (T-SQL). Lakehouse / Spark is not in scope.

---

## Prerequisites

- [Claude Code CLI](https://docs.anthropic.com/claude-code) (`claude`)
- [uv](https://docs.astral.sh/uv/) — Python package manager
- Python 3.11+
- An Anthropic API key (`ANTHROPIC_API_KEY`)
- SQL Server credentials — required for DDL extraction (`/setup-ddl`):

```bash
export MSSQL_HOST=localhost
export MSSQL_PORT=1433
export MSSQL_DB=AdventureWorksDW
export SA_PASSWORD=your-password
```

---

## Setup

Clone the repo and load the plugin:

```bash
git clone https://github.com/accelerate-data/migration-utility
cd migration-utility
claude --plugin-dir .
```

On first use, run the init command to verify and install prerequisites:

```text
/init-ad-migration
```

This checks for `uv`, Python 3.11+, shared package deps, the DDL MCP server, and SQL Server credentials. It installs anything missing after confirmation.

---

## Pipeline

### 1 — Extract DDL

Run the setup-ddl skill to connect to your SQL Server, extract stored procedure and table DDL, and build the catalog:

```text
/setup-ddl
```

This produces a DDL directory and `catalog/` JSON files that all downstream agents read.

### 2 — Scope

Run the scoping agent to identify which procedure writes to each target table:

```bash
claude --agent scoping-agent <input.json> <output.json>
```

Output is a `candidate_writers.json` file — one entry per table with the resolved writer procedure (or an explanation of why it could not be resolved).

### 3 — Profile

Run the profile skill or agent to analyse each target table's catalog signals and produce a structured profile used to drive dbt model generation:

```text
/profile
```

Or for batch profiling without approval gates, use the profiler agent directly.

### 4 — Migrate

Run the migrate skill or agent to generate dbt model SQL and schema YAML from the scoped and profiled catalog:

```text
/migrate
```

Output is ready-to-review dbt model files written to your dbt project directory.

---

## Repository Structure

```text
.claude-plugin/       Plugin manifest (marketplace.json)
bootstrap/            Bootstrap sub-plugin — init and setup-ddl
migration/            Migration sub-plugin — agents and skills
  agents/             scoping-agent, profiler-agent, migrator-agent
  skills/             discover, profile, migrate
lib/                  Python library (uv project)
  shared/             DDL analysis modules and JSON schemas
mcp/
  ddl/                DDL file MCP server (structured AST access)
  mssql/              genai-toolbox config for live SQL Server queries
tests/
  unit/               pytest suite for the shared library
docs/                 Design docs and reference guides
```

---

## Development

### Run tests

```bash
cd lib && uv run pytest
```

### Lint markdown

```bash
markdownlint <file>
```

### Commands reference

See `repo-map.json` → `commands` for the full command list.

---

## License

[Elastic License 2.0](LICENSE) — free to use, not available as a managed service.
