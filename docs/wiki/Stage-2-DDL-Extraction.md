# Stage 2 -- DDL Extraction

`ad-migration setup-source` extracts source metadata and builds the local migration catalog that every downstream command depends on. It also persists the active source endpoint in `manifest.json` as `runtime.source` and writes extraction metadata under `extraction.*`.

## Invocation

```bash
# SQL Server
ad-migration setup-source --technology sql_server --schemas silver,gold

# Oracle
ad-migration setup-source --technology oracle --schemas SH,HR
```

| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `sql_server` or `oracle` |
| `--schemas` | yes | Comma-separated list of schemas to extract |
| `--project-root` | no | Defaults to current working directory |

## What it produces

- `manifest.json`
- extracted object definitions in `ddl/`
- per-object catalog JSON in `catalog/`

## How it works

1. Validates required environment variables (exits 1 with a list of missing vars if absent)
2. Runs extraction via `run_extract`
3. Writes `manifest.json`, `ddl/`, and `catalog/`
4. Runs AST enrichment

## Prerequisites

The CLI validates all required environment variables before connecting. Missing variables cause an immediate exit with a message listing exactly which vars are absent.

### SQL Server

- `toolbox` on `PATH`
- `MSSQL_HOST`
- `MSSQL_PORT`
- `MSSQL_DB`
- `SA_PASSWORD`

These are bootstrap inputs for the initial source connection. Once `ad-migration setup-source` completes, the active source endpoint is persisted in `manifest.json` as `runtime.source`.

### Oracle

- SQLcl and Java 11+
- `ORACLE_HOST`
- `ORACLE_PORT`
- `ORACLE_SERVICE`
- `ORACLE_USER`
- `ORACLE_PASSWORD`

The Oracle user needs catalog access such as `SELECT_CATALOG_ROLE` and `SELECT ANY DICTIONARY`.

## Re-running extraction

Re-running `ad-migration setup-source` rebuilds `ddl/` and `catalog/` from source state. Enriched migration fields such as scoping, profile, and refactor are preserved across re-extraction where the CLI supports that preservation.

## Known limitation

Dynamic SQL writers may not appear in metadata-driven references. Those are resolved later during `/scope` or `/analyzing-table`.

## Next step

Proceed to [[Stage 1 Scoping]] if you want to classify writers first, or to [[Browsing the Catalog]] if you want to inspect the extracted state before acting.
