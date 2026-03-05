# Layer 1: MCP Server

We are using the MCP Toolbox for databases which is an open source MCP server for databases.

The goal of this phase is to test the MCP toolbox for databases in isolation against our local SQL Server.

Back to [Local Agent Testing overview](README.md).

---

## Goal

Verify that:

1. toolbox connects to the local SQL Server container.
2. The `mssql-execute-sql` tool executes arbitrary SQL correctly.
3. The SQL catalog queries the scoping agent will use return the expected schema and rows.

---

## Prerequisites

- Local SQL Server container running with a project database restored.
  See [Docker Setup](../setup-docker/README.md).
- `SA_PASSWORD` for the container.
- toolbox binary installed (see below).
- Claude Code or Codex with MCP support.

---

## Step 1: Install genai-toolbox

```bash
brew install mcp-toolbox
toolbox --version
```

---

## Step 2: Locate `tools.yaml`

The canonical config is checked into the repo at `orchestrator/mssql_mcp/tools.yaml`.

```text
orchestrator/mssql_mcp/tools.yaml   ← single source of truth (local + GHCR image)
orchestrator/mssql_mcp/Dockerfile   ← GHCR wrapper image (used by Layer 3)
```

The file uses `${ENV_NAME}` placeholders for all connection details. No secrets are
stored in the file.

---

## Step 3: Add to Claude Code as an MCP server

First, verify `toolbox` starts correctly from the repo root:

```bash
toolbox --stdio --tools-file orchestrator/mssql_mcp/tools.yaml
# Should hang waiting for stdin input — that means it started correctly. Ctrl-C to exit.
```

If that works, `.mcp.json` is already committed at the repo root with `SA_PASSWORD` left blank.
Fill in your password locally and tell git to ignore the change so it is never accidentally committed:

```bash
# Edit .mcp.json and set SA_PASSWORD to your container password
git update-index --skip-worktree .mcp.json
```

The committed file looks like:

```json
{
  "mcpServers": {
    "mssql": {
      "command": "toolbox",
      "args": ["--stdio", "--tools-file", "orchestrator/mssql_mcp/tools.yaml"],
      "env": {
        "MSSQL_HOST": "127.0.0.1",
        "MSSQL_PORT": "1433",
        "MSSQL_DB": "WideWorldImportersDW",
        "SA_PASSWORD": ""
      }
    }
  }
}
```

Restart Claude Code and run `/mcp` to confirm `mssql` is connected.

---

## Step 4: Run validation queries

For each query below, paste it into Claude Code with a prompt like:

> Use `mssql-execute-sql` to run this query: `<paste query here>`

Claude will call the tool and return results inline. Verify `/mcp` shows the `mssql` server
connected before starting. Confirm results match expectations before moving to Layer 2.

### 4.1 Dependency metadata (DiscoverCandidates)

Finds procedures that reference a target table via SQL Server dependency metadata.

```sql
SELECT
    OBJECT_SCHEMA_NAME(referencing_id)  AS proc_schema,
    OBJECT_NAME(referencing_id)         AS proc_name,
    OBJECT_SCHEMA_NAME(referenced_id)   AS table_schema,
    OBJECT_NAME(referenced_id)          AS table_name
FROM sys.sql_expression_dependencies
WHERE referenced_entity_name = 'your_target_table'
  AND OBJECTPROPERTY(referencing_id, 'IsProcedure') = 1;
```

Expected: rows for each stored procedure that references the table. Empty result means
no metadata-visible references (dynamic SQL or `TRUNCATE`-only writers may still exist).

### 4.2 Procedure body retrieval (ResolveCallGraph / DetectWriteOperations)

Fetches the full T-SQL body of a candidate procedure for AST parsing.

```sql
SELECT
    OBJECT_SCHEMA_NAME(o.object_id) AS schema_name,
    o.name                          AS proc_name,
    m.definition
FROM sys.sql_modules m
JOIN sys.objects o ON o.object_id = m.object_id
WHERE o.type = 'P'
  AND o.name = 'your_procedure_name';
```

Expected: one row with `definition` containing the full CREATE PROCEDURE body.

### 4.3 All procedures in database (baseline discovery)

```sql
SELECT
    ROUTINE_SCHEMA,
    ROUTINE_NAME,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;
```

Expected: full list of stored procedures in the database.

### 4.4 Cross-database reference detection

Verifies the agent can detect out-of-scope cross-database references.

```sql
SELECT DISTINCT
    OBJECT_NAME(referencing_id)         AS proc_name,
    referenced_database_name
FROM sys.sql_expression_dependencies
WHERE referenced_database_name IS NOT NULL
  AND OBJECTPROPERTY(referencing_id, 'IsProcedure') = 1;
```

Expected: any procedures with cross-database references. These should produce
`status: error` with code `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE` in the agent output.

---

## Step 5: Confirm and record results

For each query above, note:

- Does it return rows? If not, explain why (empty database, no procedures, etc.).
- Are the column names and types what the agent contract expects?
- Any permission errors from the `sa` account?

Document any deviations in a comment on the Layer 1 Linear issue before proceeding to
Layer 2.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `mssql-execute-sql` not in `/mcp` output | `.mcp.json` missing or Claude Code not restarted | Check `.mcp.json` exists at repo root, restart Claude Code |
| "configured but not connected" in Claude | `toolbox` failed to start | Run `toolbox --stdio --tools-file <abs-path>` manually to see the error; check `which toolbox` |
| Connection refused | SQL Server container not running | `docker ps`, start container |
| Login failed for user 'sa' | Wrong `SA_PASSWORD` or SA login disabled | Check container env, `docker inspect` |
| Empty results from `sys.sql_expression_dependencies` | Database restored but no procedures referencing target | Use a known procedure + table combo to sanity-check |
