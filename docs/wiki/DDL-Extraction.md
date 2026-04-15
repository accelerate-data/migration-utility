# DDL Extraction

`ad-migration setup-source` extracts source metadata and builds the local migration catalog that every downstream command depends on. It also persists the active source endpoint in `manifest.json` as `runtime.source` and writes extraction metadata under `extraction.*`.

## Invocation

```bash
ad-migration setup-source --schemas silver,gold
ad-migration setup-source --schemas SH,HR
```

| Option | Required | Description |
|---|---|---|
| `--schemas` | yes* | Comma-separated list of schemas to extract |
| `--all-schemas` | yes* | Discover and extract every schema in the source database |
| `--yes` | no | Skip the confirmation prompt used with `--all-schemas` |
| `--project-root` | no | Defaults to current working directory |

*Use one of `--schemas` or `--all-schemas`.

`setup-source` reads the source technology from `manifest.json` as `runtime.source`, which is seeded by `/init-ad-migration`.

## What it produces

- `manifest.json`
- extracted object definitions in `ddl/`
- per-object catalog JSON in `catalog/`

## How it works

1. Validates required environment variables (exits 1 with a list of missing vars if absent)
2. Verifies live extraction prerequisites for the configured technology
3. Refreshes scaffolded project files and git hooks when needed
4. Runs extraction via `run_extract`
5. Writes `manifest.json`, `ddl/`, and `catalog/`

## Prerequisites

The CLI validates all required environment variables before connecting. Missing variables cause an immediate exit with a message listing exactly which vars are absent.

### SQL Server

- FreeTDS installed and available to the CLI
- `SOURCE_MSSQL_HOST`
- `SOURCE_MSSQL_PORT`
- `SOURCE_MSSQL_DB`
- `SOURCE_MSSQL_USER`
- `SOURCE_MSSQL_PASSWORD`
- optional `MSSQL_DRIVER` override if you are not using the default `FreeTDS` driver

These are bootstrap inputs for the initial source connection. Once `ad-migration setup-source` completes, the active source endpoint is persisted in `manifest.json` as `runtime.source`.

### Oracle

- SQLcl and Java 11+
- `SOURCE_ORACLE_HOST`
- `SOURCE_ORACLE_PORT`
- `SOURCE_ORACLE_SERVICE`
- `SOURCE_ORACLE_USER`
- `SOURCE_ORACLE_PASSWORD`

The Oracle user needs catalog access such as `SELECT_CATALOG_ROLE` and `SELECT ANY DICTIONARY`.

## Re-running extraction

Re-running `ad-migration setup-source` rebuilds `ddl/` and `catalog/` from source state. Enriched migration fields such as scoping, profile, and refactor are preserved across re-extraction where the CLI supports that preservation.

## Known limitation

Dynamic SQL writers may not appear in metadata-driven references. Those are resolved later during `/scope` or `/analyzing-table`.

## Next step

Proceed to [[Scoping]] if you want to classify writers first, or to [[Browsing the Catalog]] if you want to inspect the extracted state before acting.
