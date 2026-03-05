# Integration Tests

End-to-end integration tests for the scoping agent. Each test spawns the real
`claude` CLI with the scoping agent plugin against the `MigrationTest` SQL Server
database and asserts on the output JSON.

## Prerequisites

- `aw-sql` Docker container running with `MigrationTest` database loaded
  (see [Test Database Image](../docs/reference/test-db-image/README.md))
- Shell env configured for the MCP server (same vars used when running the agent manually):
  `SA_PASSWORD`, `MSSQL_DB`, `MSSQL_HOST`, `MSSQL_PORT` — these are read by `plugin/.mcp.json`
- `ANTHROPIC_API_KEY` exported in your shell
- `claude` CLI on PATH

## Running

```bash
cd tests/
npm install
export ANTHROPIC_API_KEY=your-api-key
npm test
```

The invocation matches the overall-design local dev command exactly — the test spawns:

```bash
claude --plugin-path plugin/ --agent scoping-agent <input.json> <output.json>
```

Shell env is inherited by the subprocess. `plugin/.mcp.json` handles MCP server config.

## Scenarios

| Test | Fixture | Expected status |
|---|---|---|
| resolved | `resolved.input.json` | `resolved` — `silver.usp_load_DimProduct` |
| no_writer_found | `no-writer-found.input.json` | `no_writer_found` |
| error cross-db | `error-cross-db.input.json` | `error` — `ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE` |
| writer-through-view | `writer-through-view.input.json` | `resolved` — `silver.usp_load_DimPromotion` |
| ambiguous_multi_writer | `ambiguous-multi-writer.input.json` | `ambiguous_multi_writer` |
| partial | `partial.input.json` | `partial` |
| call-graph | `call-graph.input.json` | `resolved` — `silver.usp_stage_FactInternetSales` |
| mv-as-target | `mv-as-target.input.json` | `no_writer_found` |

## Notes

- Tests are not suitable for standard CI — they require a live SQL Server container
  and call the real Anthropic API. Each test can take up to 5 minutes.
- The `globalSetup` fails fast if prerequisites are not met.
- Fixtures live at `scripts/sql/test-fixtures/` (created by VU-422).
