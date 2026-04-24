# Test Manifest

This repo uses focused unit, integration, and agent-eval suites. Run the smallest suite that covers the files you changed.

## Unit Tests

| Suite | Command | Covers |
| --- | --- | --- |
| Shared Python | `cd lib && uv run pytest` | Shared DDL analysis library and CLI support modules under `lib/shared/` |
| DDL MCP | `cd mcp/ddl && uv run pytest` | Local DDL MCP server and standalone parser support |

## Integration Tests

| Suite | Command | Requires |
| --- | --- | --- |
| SQL Server | `cd lib && uv run pytest ../tests/integration/sql_server` | Docker SQL Server fixture and SQL Server env vars |
| Oracle | `cd lib && uv run pytest ../tests/integration/oracle` | Docker Oracle fixture and Oracle env vars |
| DDL MCP Oracle | `cd mcp/ddl && uv run pytest ../../tests/integration/oracle/ddl_mcp` | Docker Oracle fixture |

## Agent Evals

| Suite | Command | Covers |
| --- | --- | --- |
| Smoke | `cd tests/evals && npm run eval:smoke` | Curated offline promptfoo regression pass |
| Package-specific evals | `cd tests/evals && npm run eval:<package>` | Individual command and skill packages |

## Coverage

CI enforces shared-library line coverage with:

```bash
cd lib && uv run --with pytest-cov pytest --cov=shared --cov-report=term-missing --cov-fail-under=70
```

Coverage for integration and promptfoo evals is tracked by scenario behavior rather than line coverage.

## Known Gaps

- Full Ruff lint and format enforcement are not enabled yet because the existing codebase needs a dedicated formatting and lint cleanup pass.
- Live SQL Server and Oracle integration tests depend on local Docker infrastructure and are not part of the default CI path.
- Promptfoo evals are not run on every PR because they depend on model/runtime configuration and can be expensive.
