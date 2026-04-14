# Testing the ad-migration CLI

## Unit tests

All CLI unit tests live in `tests/unit/cli/`. They use `typer.testing.CliRunner` and mock the
underlying `run_*` functions — no database or real environment variables needed.

```bash
# Run all CLI unit tests
cd lib && uv run pytest ../tests/unit/cli/ -v

# Run tests for a single command
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py -v
cd lib && uv run pytest ../tests/unit/cli/test_env_check.py -v

# Full library suite — verify no regressions
cd lib && uv run pytest
```

## Dev smoke testing

Run the CLI without installing it using `uv run`:

```bash
cd lib && uv run ad-migration --help
cd lib && uv run ad-migration setup-source --help
cd lib && uv run ad-migration reset --help
```

Expected: each command prints its usage, options, and description.

## Env var validation

Test that the CLI exits 1 with a clear message when credentials are missing:

```bash
# Unset all SQL Server vars and run setup-source
env -i HOME=$HOME uv run --project lib ad-migration setup-source \
  --technology sql_server --schemas silver
echo "exit: $?"
# Expected: exit 1; stderr lists MSSQL_HOST, MSSQL_PORT, MSSQL_DB, SA_PASSWORD
```

Expected error format:

```text
Error: missing required environment variables for sql_server:

  MSSQL_HOST                     not set
  MSSQL_PORT                     not set
  MSSQL_DB                       not set
  SA_PASSWORD                    not set

Set these in your shell or .envrc before running setup-source.
```

Test with all vars set (should pass env check and show help, not extract):

```bash
MSSQL_HOST=localhost MSSQL_PORT=1433 MSSQL_DB=AdventureWorks2022 SA_PASSWORD=test \
  uv run --project lib ad-migration setup-source --help
# Expected: exit 0, help text shown
```

## Per-command manual tests (requires live database)

### setup-source

```bash
ad-migration setup-source --technology sql_server --schemas silver,gold

# Verify artifacts written
ls -la ddl/ catalog/tables/ manifest.json

# Verify --no-commit skips git commit
ad-migration setup-source --technology sql_server --schemas silver --no-commit
git status  # should show untracked/modified files, not a clean tree
```

### setup-target

```bash
# Set TARGET_* vars for your technology first
ad-migration setup-target --technology fabric
ls dbt/dbt_project.yml dbt/models/staging/sources.yml
```

### setup-sandbox / teardown-sandbox

```bash
ad-migration setup-sandbox --yes
python3 -c "import json; m=json.load(open('manifest.json')); print(m['runtime']['sandbox'])"

ad-migration teardown-sandbox --yes
python3 -c "import json; m=json.load(open('manifest.json')); print(m.get('runtime',{}).get('sandbox','cleared'))"
```

### reset

```bash
ad-migration reset profile silver.DimCustomer --yes
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimcustomer.json'))
print('profile' in cat)  # False
"
```

### exclude-table / add-source-table

```bash
ad-migration exclude-table silver.DimCurrency --no-commit
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimcurrency.json'))
print(cat.get('is_excluded'))  # True
"

ad-migration add-source-table silver.DimGeography --no-commit
python3 -c "
import json
cat = json.load(open('catalog/tables/silver.dimgeography.json'))
print(cat.get('is_source'))  # True
"
```

## Exit codes

| Code | Meaning | Example scenario |
|---|---|---|
| `0` | Success | Command completed normally |
| `1` | Domain failure | Missing env vars, invalid flag value, missing manifest.json |
| `2` | IO / connection error | Database unreachable, permissions failure |

Quick verification:

```bash
# Should exit 1 (missing env vars)
ad-migration setup-source --technology sql_server --schemas silver 2>/dev/null
echo "exit: $?"
```

## CI integration

The `--no-commit` and `--yes` flags make all commands CI-safe:

```bash
# Full setup pipeline, no prompts, no commits
ad-migration setup-source --technology sql_server --schemas silver --no-commit
ad-migration setup-target --technology fabric --no-commit
ad-migration setup-sandbox --yes
```

Use `set -e` or check `$?` after each step — exit codes are reliable.
