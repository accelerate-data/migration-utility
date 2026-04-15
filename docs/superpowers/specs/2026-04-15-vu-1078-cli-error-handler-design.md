# VU-1078: CLI Error Handler Design

## Problem

`ad-migration` commands surface unhandled exceptions as raw Python tracebacks. Commands catch `OSError` and `ConnectionError` but miss driver-level exceptions (`pyodbc.Error`, `oracledb.DatabaseError`) which propagate to the terminal as multi-line stack traces. `pretty_exceptions_enable=False` in `main.py` disables Typer's formatter, so nothing intercepts them.

## Goal

All `ad-migration` commands show a clean two-line error with an actionable hint when an exception escapes command-level handling:

```text
✗ Error while connecting to SQL Server: Cannot open database "[KimballFixture]".
  Hint: Verify MSSQL_DB is set to a valid database name and SA_PASSWORD grants access.
```

## Approach: shared `cli_error_handler` context manager

One new file holds the exception-to-hint mapping and the context manager. Commands wrap risky external call sites with it. No changes to `main.py`, no changes to the `output` module, no removal of existing `try/except` blocks.

## New file: `lib/shared/cli/error_handler.py`

```python
@contextmanager
def cli_error_handler(operation: str) -> Iterator[None]:
    try:
        yield
    except <MappedException> as exc:
        error(f"Error while {operation}: {exc}")
        console.print(f"  Hint: {hint_for(exc)}")
        raise typer.Exit(code=exit_code_for(exc))
```

### Exception mapping

| Exception | Exit code | Hint |
|---|---|---|
| `pyodbc.ProgrammingError` | 2 | Check `MSSQL_DB` is a valid database name and `SA_PASSWORD` grants access |
| `pyodbc.OperationalError` | 2 | Check `MSSQL_HOST`, `MSSQL_PORT`, and network connectivity |
| `pyodbc.InterfaceError` | 2 | ODBC Driver 18 for SQL Server may not be installed — run `brew install msodbcsql18` |
| `pyodbc.Error` (fallback) | 2 | Check SQL Server connection environment variables |
| `oracledb.DatabaseError` | 2 | Check Oracle connection environment variables (`ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_USER`, `ORACLE_PASSWORD`) |
| `OSError` | 2 | Check network connectivity or file path permissions |
| `ConnectionError` | 2 | Check host, port, and network access |
| `ValueError` | 1 | Uses the exception message directly — no separate hint |
| Any other `Exception` | 1 | Shows message only, no hint, no traceback |

`pyodbc` and `oracledb` imports are guarded with `try/except ImportError` so the handler works even if a driver is not installed in the current environment.

## Call sites

Each command wraps calls to external systems that currently escape to raw tracebacks:

| Command | Call site to wrap |
|---|---|
| `setup_source_cmd.py` | `run_list_schemas`, `run_extract` |
| `setup_target_cmd.py` | target resolution and `run_generate_sources` |
| `setup_sandbox_cmd.py` | `backend.sandbox_up` |
| `teardown_sandbox_cmd.py` | `backend.sandbox_down` |
| `reset_cmd.py` | `_create_backend`, `backend.sandbox_down` inside `_teardown_sandbox_if_configured` |
| `exclude_table_cmd.py` | `run_exclude` |
| `add_source_table_cmd.py` | `run_ready`, `run_write_source` |

Existing `try/except` blocks are preserved — `cli_error_handler` is additive.

## Output format

Normal (exit 2 — connection/IO failure):

```text
✗ Error while <operation>: <exception message>.
  Hint: <actionable hint>.
```

Domain error (exit 1 — ValueError):

```text
✗ Error while <operation>: <exception message>.
```

Unknown exception (exit 1):

```text
✗ Unexpected error while <operation>: <exception message>.
```

## Testing

New file: `tests/unit/cli/test_error_handler.py`

One test per exception type verifying:

- Correct exit code
- Output contains the operation string
- Output contains the expected hint text

No per-command retesting needed — the mapping is centralised.

## Files changed

| File | Change |
|---|---|
| `lib/shared/cli/error_handler.py` | New — context manager + exception mapping |
| `lib/shared/cli/setup_source_cmd.py` | Wrap `run_list_schemas`, `run_extract` |
| `lib/shared/cli/setup_target_cmd.py` | Wrap target resolution calls |
| `lib/shared/cli/setup_sandbox_cmd.py` | Wrap `backend.sandbox_up` |
| `lib/shared/cli/teardown_sandbox_cmd.py` | Wrap `backend.sandbox_down` |
| `lib/shared/cli/reset_cmd.py` | Wrap backend calls in `_teardown_sandbox_if_configured` |
| `lib/shared/cli/exclude_table_cmd.py` | Wrap `run_exclude` |
| `lib/shared/cli/add_source_table_cmd.py` | Wrap `run_ready`, `run_write_source` |
| `tests/unit/cli/test_error_handler.py` | New — unit tests for each exception mapping |

## Linear issue

VU-1078
