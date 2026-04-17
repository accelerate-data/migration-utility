# Testing the ad-migration CLI

Developer-only reference for validating the `ad-migration` CLI locally.

## Unit tests

All CLI unit tests live in `tests/unit/cli/`. They use `typer.testing.CliRunner` and mock the
underlying `run_*` functions, so no database is required. Run them from the shared `lib` project
environment, which actually has pytest available:

```bash
cd lib && uv run pytest ../tests/unit/cli/ -v
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py -v
cd lib && uv run pytest ../tests/unit/cli/test_env_check.py -v
cd lib && uv run pytest
```

## Dev smoke testing

Run the CLI without installing it:

```bash
cd packages/ad-migration-cli && uv run ad-migration --help
cd packages/ad-migration-cli && uv run ad-migration setup-source --help
cd packages/ad-migration-cli && uv run ad-migration reset --help
```

Expected: each command prints usage, options, and description.

## Env var validation

Check that `setup-source` fails cleanly when required variables are missing:

```bash
env -i HOME=$HOME uv run --project packages/ad-migration-cli ad-migration setup-source \
  --schemas silver
echo "exit: $?"
```

Expected: exit `1`; stderr lists the missing `SOURCE_MSSQL_*` variables.

With variables set, the command should pass env validation and show help:

```bash
SOURCE_MSSQL_HOST=localhost SOURCE_MSSQL_PORT=1433 SOURCE_MSSQL_DB=AdventureWorks2022 SOURCE_MSSQL_PASSWORD=test \
  uv run --project packages/ad-migration-cli ad-migration setup-source --help
```

## Manual command checks

These require a live database and valid local configuration.

## Homebrew release verification

Verify the published install contract before cutting or approving a release:

1. Build `ad-migration-shared` and `ad-migration-cli`.
2. Confirm `ad-migration --version` works in a fresh virtualenv after installing the built wheels.
3. Render `Formula/ad-migration.rb` from the built sdists and install it with Homebrew from source.
4. Confirm `ad-migration --version` succeeds after the Homebrew install and `brew test ad-migration` passes.
5. Run `/init-ad-migration` in a clean plugin checkout and verify the command skips Homebrew installation when the binary is already present.

### `setup-source`

```bash
ad-migration setup-source --schemas silver,gold
ls -la ddl/ catalog/tables/ manifest.json
```

### `setup-target`

```bash
ad-migration setup-target
ls dbt/dbt_project.yml dbt/models/staging/_staging__sources.yml
```

### `setup-sandbox` / `teardown-sandbox`

```bash
ad-migration setup-sandbox --yes
python3 -c "import json; m=json.load(open('manifest.json')); print(m['runtime']['sandbox'])"

ad-migration teardown-sandbox --yes
python3 -c "import json; m=json.load(open('manifest.json')); print(m.get('runtime',{}).get('sandbox','cleared'))"
```

### `reset`

```bash
ad-migration reset profile silver.DimCustomer --yes
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimcustomer.json'))
print('profile' in cat)
"
```

### `exclude-table` / `add-source-table`

```bash
ad-migration exclude-table silver.DimCurrency
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimcurrency.json'))
print(cat.get('is_excluded'))
"

ad-migration add-source-table silver.DimGeography
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimgeography.json'))
print(cat.get('is_source'))
"
```

## Exit codes

| Code | Meaning | Example scenario |
|---|---|---|
| `0` | Success | Command completed normally |
| `1` | Domain failure | Missing env vars, invalid flag value, missing manifest |
| `2` | IO / connection error | Database unreachable, permissions failure |
