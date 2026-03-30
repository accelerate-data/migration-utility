# Running Tests

How to run each test suite manually from the repository root.

## Prerequisites

| Tool | Install |
|---|---|
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker (integration tests only) | Docker Desktop |

## Python shared library

Tests live in `tests/unit/`. They test the shared Python library (DDL parsing, object discovery, profiling, migration).

```bash
# All unit tests
cd lib && uv run pytest

# Single test file
cd lib && uv run pytest ../tests/unit/test_discover.py -v

# Single test
cd lib && uv run pytest ../tests/unit/test_smoke.py::test_load_directory_mixed_types_single_file -v
```

## MCP server (DDL)

```bash
cd mcp/ddl && uv run pytest
```

## Integration tests (SQL Server)

Requires a local SQL Server container (see [Docker Setup](../setup-docker/README.md)).

```bash
cd lib && uv run pytest -m integration
```

## Quick reference

| What changed | Command |
|---|---|
| Python shared lib (loader, discover, profile, migrate) | `cd lib && uv run pytest` |
| DDL MCP server | `cd mcp/ddl && uv run pytest` |
| SQL Server integration | `cd lib && uv run pytest -m integration` |
