# Stage 2 -- DDL Extraction

`/setup-ddl` extracts source metadata and builds the local migration catalog that every downstream command depends on.

## What it produces

- `manifest.json`
- extracted object definitions in `ddl/`
- per-object catalog JSON in `catalog/`

## SQL Server / Fabric flow

For SQL Server-style sources, `/setup-ddl` runs an interactive extraction flow:

1. choose the source database
2. choose one or more schemas
3. confirm the extraction preview
4. extract DDL and write catalog state
5. run enrichment so catalog references include both database metadata and AST-derived signals

### Prerequisites

- `toolbox` on `PATH`
- `MSSQL_HOST`
- `MSSQL_PORT`
- `MSSQL_DB`
- `SA_PASSWORD`

These need to be available before the Claude session starts so the MCP server can read them.

## Oracle flow

For Oracle projects, the usual path is the headless CLI extract flow rather than the interactive SQL Server-style prompt flow:

```bash
uv run --project <shared-path> setup-ddl extract --schemas SH,HR
```

The Oracle user needs catalog access such as `SELECT_CATALOG_ROLE` and `SELECT ANY DICTIONARY`.

## Re-running extraction

Re-running `/setup-ddl` rebuilds `ddl/` and `catalog/` from source state. Enriched migration fields such as scoping, profile, and refactor are preserved across re-extraction where the CLI supports that preservation.

## Known limitation

Dynamic SQL writers may not appear in metadata-driven references. Those are resolved later during `/scope` or `/analyzing-table`.

## Next step

Proceed to [[Stage 1 Scoping]] if you want to classify writers first, or to [[Browsing the Catalog]] if you want to inspect the extracted state before acting.
