# Integration Test Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make live integration tests use the documented role-specific source, sandbox, and target contracts while keeping extraction constrained to the canonical `MigrationTest` fixture schema.

**Architecture:** Keep bootstrapping explicit per technology. Add small shared env-validation helpers, keep `materialize_migration_test()` as the shared fixture entry point, and normalize SQL Server and Oracle test helpers around `SOURCE_*`, `SANDBOX_*`, and `TARGET_*` roles. SQL Server uses FreeTDS only; `MSSQL_DRIVER` is removed from runtime and integration-test configuration.

**Tech Stack:** Python 3, pytest, uv, pyodbc with FreeTDS, oracledb, shell fixture scripts, markdownlint.

---

## File Structure

- Modify `lib/shared/db_connect.py`: hard-code SQL Server ODBC driver to `FreeTDS`; remove `MSSQL_DRIVER` env reads from source connections.
- Modify `lib/shared/dbops/sql_server.py`: make fixture materialization and target setup use FreeTDS by default without env override.
- Modify `lib/shared/setup_ddl_support/manifest.py`: remove SQL Server driver from source identity snapshots.
- Modify `lib/shared/cli/setup_sandbox_cmd.py`: remove `MSSQL_DRIVER` from sandbox runtime writes.
- Modify `lib/shared/sandbox/sql_server_services.py`: default sandbox and source connections to FreeTDS only.
- Modify `tests/integration/runtime_helpers.py`: add role-specific env validation and build source/sandbox roles from `SOURCE_*` and `SANDBOX_*` only.
- Modify SQL Server integration tests under `tests/integration/sql_server/`: use role-specific helpers and extract only `MigrationTest`.
- Modify Oracle integration tests under `tests/integration/oracle/`: use role-specific helpers and `SOURCE_ORACLE_*` names consistently.
- Modify `tests/unit/fixture_materialization/test_fixture_materialization.py`: assert materialization uses sandbox/admin role env and no legacy SQL Server driver override.
- Modify unit tests under `tests/unit/cli/`, `tests/unit/setup_ddl/`, and sandbox-related unit test files as needed when driver and env contracts change.
- Modify `.env.example`, `.envrc`, `docs/reference/setup-docker/README.md`, and SQL Server wiki docs to show the role-specific env contract and remove `MSSQL_DRIVER`.

---

### Task 1: Make SQL Server Source Connections FreeTDS-Only

**Files:**

- Modify: `lib/shared/db_connect.py`
- Modify: `tests/unit/setup_ddl/test_manifest_and_handoff.py`
- Test: `tests/unit/setup_ddl/test_manifest_and_handoff.py`

- [ ] **Step 1: Write failing tests for source identity without driver override**

Add or update a test in `tests/unit/setup_ddl/test_manifest_and_handoff.py` that proves SQL Server source identity no longer includes `driver` and ignores `MSSQL_DRIVER`.

```python
def test_sql_server_connection_identity_omits_driver(monkeypatch):
    from shared.setup_ddl_support.manifest import get_connection_identity

    monkeypatch.setenv("SOURCE_MSSQL_HOST", "source-host")
    monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "readonly_user")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "readonly-password")
    monkeypatch.setenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    identity = get_connection_identity("sql_server", "WarehouseDb")

    assert identity["connection"]["host"] == "source-host"
    assert identity["connection"]["database"] == "WarehouseDb"
    assert identity["connection"]["user"] == "readonly_user"
    assert identity["connection"]["password_env"] == "SOURCE_MSSQL_PASSWORD"
    assert "driver" not in identity["connection"]
```

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_manifest_and_handoff.py::test_sql_server_connection_identity_omits_driver -v
```

Expected: FAIL because `get_connection_identity()` still includes a `driver` field from `MSSQL_DRIVER`.

- [ ] **Step 3: Hard-code FreeTDS in source connection factory**

In `lib/shared/db_connect.py`, add a module constant and remove the env read:

```python
SQL_SERVER_ODBC_DRIVER = "FreeTDS"
```

Change `sql_server_connect()` so it uses the constant:

```python
driver = SQL_SERVER_ODBC_DRIVER
```

Keep `build_sql_server_connection_string()` accepting `driver`; target and sandbox callers still pass the constant explicitly until later tasks finish.

- [ ] **Step 4: Remove SQL Server driver from source identity**

In `lib/shared/setup_ddl_support/manifest.py`, update the SQL Server branch in `get_connection_identity()`:

```python
connection=RuntimeConnection(
    host=os.environ.get("SOURCE_MSSQL_HOST", "") or None,
    port=os.environ.get("SOURCE_MSSQL_PORT", "") or None,
    database=database or None,
    user=os.environ.get("SOURCE_MSSQL_USER", "") or None,
    password_env="SOURCE_MSSQL_PASSWORD",
),
```

- [ ] **Step 5: Run the focused test**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_manifest_and_handoff.py::test_sql_server_connection_identity_omits_driver -v
```

Expected: PASS.

- [ ] **Step 6: Run source-connection unit coverage**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl -v
```

Expected: PASS. If tests fail because they assert a SQL Server `driver` field, update those assertions to match the FreeTDS-only contract.

- [ ] **Step 7: Commit**

```bash
git add lib/shared/db_connect.py lib/shared/setup_ddl_support/manifest.py tests/unit/setup_ddl/test_manifest_and_handoff.py
git commit -m "Remove SQL Server driver override from source identity"
```

---

### Task 2: Remove SQL Server Driver Override From Sandbox And Target Runtime Writes

**Files:**

- Modify: `lib/shared/cli/setup_sandbox_cmd.py`
- Modify: `lib/shared/sandbox/sql_server_services.py`
- Modify: `lib/shared/dbops/sql_server.py`
- Modify: `lib/shared/target_setup.py`
- Test: `tests/unit/cli/test_sandbox_cmds.py`
- Test: target setup unit tests under `tests/unit/target_setup/` if present, otherwise run `tests/unit/cli` and `tests/unit/fixture_materialization`

- [ ] **Step 1: Write failing sandbox manifest test**

In `tests/unit/cli/test_sandbox_cmds.py`, add or update a test for `_build_sandbox_connection_manifest()`:

```python
def test_build_sql_server_sandbox_connection_omits_driver(monkeypatch):
    from shared.cli.setup_sandbox_cmd import _build_sandbox_connection_manifest

    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox_admin")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "sandbox-password")
    monkeypatch.setenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        }
    }

    updated = _build_sandbox_connection_manifest(manifest, "sql_server")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["host"] == "sandbox-host"
    assert connection["user"] == "sandbox_admin"
    assert connection["password_env"] == "SANDBOX_MSSQL_PASSWORD"
    assert "driver" not in connection
```

- [ ] **Step 2: Run the focused failing test**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py::test_build_sql_server_sandbox_connection_omits_driver -v
```

Expected: FAIL because the sandbox manifest still includes `driver`.

- [ ] **Step 3: Remove driver from setup-sandbox manifest writes**

In `lib/shared/cli/setup_sandbox_cmd.py`, change the SQL Server `RuntimeConnection` construction to:

```python
connection = RuntimeConnection(
    host=os.environ.get("SANDBOX_MSSQL_HOST") or None,
    port=os.environ.get("SANDBOX_MSSQL_PORT") or None,
    database=sandbox_role.connection.database,
    user=os.environ.get("SANDBOX_MSSQL_USER") or None,
    password_env="SANDBOX_MSSQL_PASSWORD",
)
```

- [ ] **Step 4: Hard-code FreeTDS in SQL Server sandbox services**

In `lib/shared/sandbox/sql_server_services.py`, import the shared constant:

```python
from shared.db_connect import SQL_SERVER_ODBC_DRIVER, build_sql_server_connection_string
```

Change constructor defaults and `from_env()` fallbacks:

```python
driver: str = SQL_SERVER_ODBC_DRIVER
```

```python
sandbox_driver = SQL_SERVER_ODBC_DRIVER
source_driver = SQL_SERVER_ODBC_DRIVER
```

- [ ] **Step 5: Hard-code FreeTDS in SQL Server DB operations**

In `lib/shared/dbops/sql_server.py`, import the shared constant:

```python
from shared.db_connect import SQL_SERVER_ODBC_DRIVER, build_sql_server_connection_string
```

Remove `MSSQL_DRIVER` from `materialize_migration_test_env()`:

```python
env = {
    "MSSQL_HOST": self.role.connection.host or "localhost",
    "MSSQL_PORT": self.role.connection.port or "1433",
    "MSSQL_DB": self.environment_name(),
    "MSSQL_SCHEMA": self.role.connection.schema_name or "MigrationTest",
}
```

Change `_connect()` to:

```python
driver = SQL_SERVER_ODBC_DRIVER
```

- [ ] **Step 6: Hard-code FreeTDS in target setup profiles**

In `lib/shared/target_setup.py`, find `_render_profiles_yml()` and set the SQL Server driver line from the constant or literal `FreeTDS`:

```python
driver = "FreeTDS"
```

If unit tests expect `ODBC Driver 18 for SQL Server`, update them to expect `FreeTDS`.

- [ ] **Step 7: Run focused and affected unit tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py::test_build_sql_server_sandbox_connection_omits_driver -v
cd lib && uv run pytest ../tests/unit/fixture_materialization/test_fixture_materialization.py -v
cd lib && uv run pytest ../tests/unit/cli -v
```

Expected: PASS. Any remaining failure mentioning `MSSQL_DRIVER` should be updated to the FreeTDS-only contract.

- [ ] **Step 8: Commit**

```bash
git add lib/shared/cli/setup_sandbox_cmd.py lib/shared/sandbox/sql_server_services.py lib/shared/dbops/sql_server.py lib/shared/target_setup.py tests/unit/cli/test_sandbox_cmds.py tests/unit/fixture_materialization/test_fixture_materialization.py
git commit -m "Use FreeTDS-only SQL Server runtime configuration"
```

---

### Task 3: Add Role-Specific Integration Env Helpers

**Files:**

- Modify: `tests/integration/runtime_helpers.py`
- Test: `tests/unit/fixture_materialization/test_fixture_materialization.py`

- [ ] **Step 1: Write failing unit tests for role-specific env validation**

Add tests to `tests/unit/fixture_materialization/test_fixture_materialization.py` or create `tests/unit/integration/test_runtime_helpers.py` if imports are cleaner. Use this content:

```python
import pytest

from tests.integration import runtime_helpers


def test_require_env_names_missing_role_variables(monkeypatch):
    monkeypatch.delenv("SOURCE_MSSQL_HOST", raising=False)
    monkeypatch.delenv("SOURCE_MSSQL_PASSWORD", raising=False)

    with pytest.raises(pytest.skip.Exception) as exc:
        runtime_helpers.require_env("source", ["SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PASSWORD"])

    assert "source env missing: SOURCE_MSSQL_HOST, SOURCE_MSSQL_PASSWORD" in str(exc.value)


def test_require_env_accepts_set_role_variables(monkeypatch):
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")

    runtime_helpers.require_env("source", ["SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PASSWORD"])
```

- [ ] **Step 2: Run the focused failing tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/fixture_materialization/test_fixture_materialization.py::test_require_env_names_missing_role_variables ../tests/unit/fixture_materialization/test_fixture_materialization.py::test_require_env_accepts_set_role_variables -v
```

Expected: FAIL because `require_env()` does not exist.

- [ ] **Step 3: Add shared env validation helper**

In `tests/integration/runtime_helpers.py`, add:

```python
def require_env(role: str, variable_names: list[str] | tuple[str, ...]) -> None:
    missing = [name for name in variable_names if not os.environ.get(name)]
    if missing:
        pytest.skip(f"{role} env missing: {', '.join(missing)}")
```

- [ ] **Step 4: Add SQL Server role constants**

In `tests/integration/runtime_helpers.py`, replace legacy fixture defaults with role-specific constants:

```python
SQL_SERVER_MIGRATION_DATABASE = os.environ.get("SOURCE_MSSQL_DB", "AdventureWorks2022")
SQL_SERVER_MIGRATION_SCHEMA = "MigrationTest"
SQL_SERVER_SOURCE_ENV = (
    "SOURCE_MSSQL_HOST",
    "SOURCE_MSSQL_DB",
    "SOURCE_MSSQL_USER",
    "SOURCE_MSSQL_PASSWORD",
)
SQL_SERVER_SANDBOX_ENV = (
    "SANDBOX_MSSQL_HOST",
    "SANDBOX_MSSQL_USER",
    "SANDBOX_MSSQL_PASSWORD",
)
```

Do not read `MSSQL_HOST`, `MSSQL_USER`, `SA_PASSWORD`, or `MSSQL_DRIVER` in these helpers.

- [ ] **Step 5: Add Oracle role constants**

In `tests/integration/runtime_helpers.py`, add:

```python
ORACLE_MIGRATION_SCHEMA = os.environ.get("SOURCE_ORACLE_SCHEMA", "MIGRATIONTEST").upper()
ORACLE_SOURCE_ENV = (
    "SOURCE_ORACLE_HOST",
    "SOURCE_ORACLE_SERVICE",
    "SOURCE_ORACLE_USER",
    "SOURCE_ORACLE_PASSWORD",
)
ORACLE_SANDBOX_ENV = (
    "SANDBOX_ORACLE_HOST",
    "SANDBOX_ORACLE_SERVICE",
    "SANDBOX_ORACLE_USER",
    "SANDBOX_ORACLE_PASSWORD",
)
```

Do not use `ORACLE_ADMIN_USER`, `ORACLE_PWD`, `ORACLE_SCHEMA_PASSWORD`, `ORACLE_USER`, or `ORACLE_PASSWORD` in the integration helper contract.

- [ ] **Step 6: Run the focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/fixture_materialization/test_fixture_materialization.py::test_require_env_names_missing_role_variables ../tests/unit/fixture_materialization/test_fixture_materialization.py::test_require_env_accepts_set_role_variables -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add tests/integration/runtime_helpers.py tests/unit/fixture_materialization/test_fixture_materialization.py
git commit -m "Add role-specific integration env validation"
```

---

### Task 4: Normalize SQL Server Integration Helpers To Source And Sandbox Roles

**Files:**

- Modify: `tests/integration/runtime_helpers.py`
- Modify: `tests/integration/sql_server/fixture_materialization/test_fixture_materialization_sql_server.py`
- Modify: `tests/integration/sql_server/setup_ddl/test_setup_ddl_sql_server.py`
- Modify: `tests/integration/sql_server/test_harness/test_test_harness_sql_server.py`
- Modify: `tests/integration/sql_server/test_harness/test_compare_sql_sql_server.py`
- Modify: `tests/integration/sql_server/catalog_diff/test_catalog_diff_sql_server.py`
- Modify: `tests/integration/sql_server/target_setup/test_target_setup_sql_server.py`
- Test: SQL Server integration tests listed above

- [ ] **Step 1: Update SQL Server source connection helper**

In `tests/integration/runtime_helpers.py`, replace `build_sql_server_connection_string()` with:

```python
def build_sql_server_connection_string(
    *,
    database: str = SQL_SERVER_MIGRATION_DATABASE,
    login_timeout: int | None = None,
) -> str:
    return _build_sql_server_connection_string(
        host=os.environ["SOURCE_MSSQL_HOST"],
        port=os.environ.get("SOURCE_MSSQL_PORT", "1433"),
        database=database,
        user=os.environ["SOURCE_MSSQL_USER"],
        password=os.environ["SOURCE_MSSQL_PASSWORD"],
        driver="FreeTDS",
        login_timeout=login_timeout,
    )
```

- [ ] **Step 2: Update SQL Server materialization role to sandbox/admin env**

In `tests/integration/runtime_helpers.py`, replace `build_sql_server_source_role()` with `build_sql_server_fixture_admin_role()`:

```python
def build_sql_server_fixture_admin_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ["SANDBOX_MSSQL_HOST"],
            port=os.environ.get("SANDBOX_MSSQL_PORT", "1433"),
            database=SQL_SERVER_MIGRATION_DATABASE,
            schema=SQL_SERVER_MIGRATION_SCHEMA,
            user=os.environ["SANDBOX_MSSQL_USER"],
            password_env="SANDBOX_MSSQL_PASSWORD",
        ),
    )
```

- [ ] **Step 3: Update SQL Server sandbox manifest helper**

In `tests/integration/runtime_helpers.py`, rewrite `build_sql_server_sandbox_manifest()`:

```python
def build_sql_server_sandbox_manifest() -> dict[str, object]:
    return {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["SOURCE_MSSQL_HOST"],
                    "port": os.environ.get("SOURCE_MSSQL_PORT", "1433"),
                    "database": SQL_SERVER_MIGRATION_DATABASE,
                    "schema": SQL_SERVER_MIGRATION_SCHEMA,
                    "user": os.environ["SOURCE_MSSQL_USER"],
                    "password_env": "SOURCE_MSSQL_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["SANDBOX_MSSQL_HOST"],
                    "port": os.environ.get("SANDBOX_MSSQL_PORT", "1433"),
                    "user": os.environ["SANDBOX_MSSQL_USER"],
                    "password_env": "SANDBOX_MSSQL_PASSWORD",
                },
            },
        }
    }
```

- [ ] **Step 4: Update SQL Server availability checks**

In `tests/integration/runtime_helpers.py`, replace `sql_server_is_available()`:

```python
def sql_server_is_available(pyodbc_module: Any) -> bool:
    if any(not os.environ.get(name) for name in SQL_SERVER_SOURCE_ENV):
        return False
    try:
        conn = pyodbc_module.connect(
            build_sql_server_connection_string(
                database=SQL_SERVER_MIGRATION_DATABASE,
                login_timeout=1,
            ),
            autocommit=True,
        )
        conn.close()
        return True
    except pyodbc_module.Error:
        return False
```

Add:

```python
def sql_server_sandbox_is_available(pyodbc_module: Any) -> bool:
    if any(not os.environ.get(name) for name in SQL_SERVER_SANDBOX_ENV):
        return False
    try:
        conn = pyodbc_module.connect(
            _build_sql_server_connection_string(
                host=os.environ["SANDBOX_MSSQL_HOST"],
                port=os.environ.get("SANDBOX_MSSQL_PORT", "1433"),
                database=os.environ.get("SANDBOX_MSSQL_ADMIN_DATABASE", "master"),
                user=os.environ["SANDBOX_MSSQL_USER"],
                password=os.environ["SANDBOX_MSSQL_PASSWORD"],
                driver="FreeTDS",
                login_timeout=1,
            ),
            autocommit=True,
        )
        conn.close()
        return True
    except pyodbc_module.Error:
        return False
```

- [ ] **Step 5: Update materialization helper**

In `tests/integration/runtime_helpers.py`, update `ensure_sql_server_migration_test_materialized()`:

```python
def ensure_sql_server_migration_test_materialized() -> None:
    global _SQL_SERVER_MIGRATION_TEST_READY
    if _SQL_SERVER_MIGRATION_TEST_READY:
        return

    require_env("source", SQL_SERVER_SOURCE_ENV)
    require_env("sandbox", SQL_SERVER_SANDBOX_ENV)
    role = build_sql_server_fixture_admin_role()
    result = materialize_migration_test(role, REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "SQL Server MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _SQL_SERVER_MIGRATION_TEST_READY = True
```

- [ ] **Step 6: Replace local SQL Server helper duplication in test-harness tests**

In `tests/integration/sql_server/test_harness/test_test_harness_sql_server.py`, remove the local `_ensure_sql_server_fixture_materialized()` and `_make_backend()` role construction. Import and use:

```python
from tests.integration.runtime_helpers import (
    build_sql_server_sandbox_manifest,
    ensure_sql_server_migration_test_materialized,
    sql_server_sandbox_is_available,
)
```

Set `_make_backend()` to:

```python
def _make_backend() -> SqlServerSandbox:
    ensure_sql_server_migration_test_materialized()
    return SqlServerSandbox.from_env(build_sql_server_sandbox_manifest())
```

Set skip marker to:

```python
skip_no_mssql = pytest.mark.skipif(
    not sql_server_sandbox_is_available(pyodbc),
    reason="SQL Server sandbox env not configured or not reachable",
)
```

Apply the same helper usage in `tests/integration/sql_server/test_harness/test_compare_sql_sql_server.py`.

- [ ] **Step 7: Update fixture materialization tests to use sandbox/admin role**

In `tests/integration/sql_server/fixture_materialization/test_fixture_materialization_sql_server.py`, replace `_build_sql_server_fixture_role()` with an import of `build_sql_server_fixture_admin_role`. Keep object assertions unchanged.

- [ ] **Step 8: Update setup-ddl SQL Server tests to require source env and materialized fixture**

In `tests/integration/sql_server/setup_ddl/test_setup_ddl_sql_server.py`, import `ensure_sql_server_migration_test_materialized` and call it before `list-schemas` and `extract` commands:

```python
ensure_sql_server_migration_test_materialized()
```

Use `SQL_SERVER_FIXTURE_DATABASE` only as the configured source database. Keep every extract command as:

```python
"--schemas", SQL_SERVER_FIXTURE_SCHEMA,
```

- [ ] **Step 9: Fix catalog-diff schema discovery**

In `tests/integration/sql_server/catalog_diff/test_catalog_diff_sql_server.py`, replace `_get_schemas()` with:

```python
def _get_schemas(self, conn: pyodbc.Connection) -> str:
    rows = _query_rows(
        conn,
        f"""
        SELECT COUNT(*) AS object_count
        FROM sys.objects
        WHERE schema_id = SCHEMA_ID('{SQL_SERVER_FIXTURE_SCHEMA}')
          AND is_ms_shipped = 0
        """,
    )
    assert rows[0]["object_count"] > 0, f"No objects found in {SQL_SERVER_FIXTURE_SCHEMA}"
    return SQL_SERVER_FIXTURE_SCHEMA
```

This keeps catalog-diff extraction fixed to `MigrationTest`.

- [ ] **Step 10: Update target setup SQL Server env usage**

In `tests/integration/sql_server/target_setup/test_target_setup_sql_server.py`, keep target-side `bronze` assertions. Replace source credentials with `SOURCE_MSSQL_*` and target credentials with `TARGET_MSSQL_*`. Remove `MSSQL_DRIVER` from generated manifest connections.

Use this source connection block in `_write_manifest()`:

```python
"connection": {
    "host": os.environ["SOURCE_MSSQL_HOST"],
    "port": os.environ.get("SOURCE_MSSQL_PORT", "1433"),
    "database": os.environ["SOURCE_MSSQL_DB"],
    "schema": SQL_SERVER_FIXTURE_SCHEMA,
    "user": os.environ["SOURCE_MSSQL_USER"],
    "password_env": "SOURCE_MSSQL_PASSWORD",
},
```

Use this target connection block:

```python
"connection": {
    "host": os.environ["TARGET_MSSQL_HOST"],
    "port": os.environ.get("TARGET_MSSQL_PORT", "1433"),
    "database": target_database,
    "user": os.environ["TARGET_MSSQL_USER"],
    "password_env": "TARGET_MSSQL_PASSWORD",
},
```

- [ ] **Step 11: Run SQL Server unit and integration smoke**

Run:

```bash
cd lib && uv run pytest ../tests/unit/fixture_materialization/test_fixture_materialization.py -v
cd lib && uv run pytest ../tests/integration/sql_server/fixture_materialization -v
cd lib && uv run pytest ../tests/integration/sql_server/setup_ddl -v
cd lib && uv run pytest ../tests/integration/sql_server/catalog_diff -v
```

Expected: PASS when role-specific SQL Server env is configured. Expected SKIP with a role-specific message when env is missing.

- [ ] **Step 12: Commit**

```bash
git add tests/integration/runtime_helpers.py tests/integration/sql_server tests/unit/fixture_materialization/test_fixture_materialization.py
git commit -m "Normalize SQL Server integration role env"
```

---

### Task 5: Normalize Oracle Integration Helpers To Source And Sandbox Roles

**Files:**

- Modify: `tests/integration/runtime_helpers.py`
- Modify: `tests/integration/oracle/conftest.py`
- Modify: `tests/integration/oracle/fixture_materialization/test_fixture_materialization_oracle.py`
- Modify: `tests/integration/oracle/setup_ddl/test_setup_ddl_oracle.py`
- Modify: `tests/integration/oracle/ddl_mcp/test_server_oracle.py`
- Modify: `tests/integration/oracle/catalog_enrich/test_catalog_enrich_oracle.py`
- Modify: `tests/integration/oracle/test_harness/test_test_harness_oracle.py`

- [ ] **Step 1: Update Oracle DSN helpers**

In `tests/integration/runtime_helpers.py`, replace `build_oracle_dsn()`:

```python
def build_oracle_dsn() -> str:
    return (
        f"{os.environ['SOURCE_ORACLE_HOST']}:"
        f"{os.environ.get('SOURCE_ORACLE_PORT', '1521')}/"
        f"{os.environ['SOURCE_ORACLE_SERVICE']}"
    )
```

Add sandbox DSN helper:

```python
def build_oracle_sandbox_dsn() -> str:
    return (
        f"{os.environ['SANDBOX_ORACLE_HOST']}:"
        f"{os.environ.get('SANDBOX_ORACLE_PORT', '1521')}/"
        f"{os.environ['SANDBOX_ORACLE_SERVICE']}"
    )
```

- [ ] **Step 2: Replace Oracle admin role helper with fixture admin role**

In `tests/integration/runtime_helpers.py`, replace `build_oracle_admin_role()` with:

```python
def build_oracle_fixture_admin_role() -> RuntimeRole:
    return RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ["SANDBOX_ORACLE_HOST"],
            port=os.environ.get("SANDBOX_ORACLE_PORT", "1521"),
            service=os.environ["SANDBOX_ORACLE_SERVICE"],
            user=os.environ["SANDBOX_ORACLE_USER"],
            schema=ORACLE_MIGRATION_SCHEMA,
            password_env="SANDBOX_ORACLE_PASSWORD",
        ),
    )
```

- [ ] **Step 3: Update Oracle admin connect kwargs for tests**

Replace `build_oracle_admin_connect_kwargs()` with:

```python
def build_oracle_sandbox_admin_connect_kwargs(oracledb_module: Any) -> dict[str, object]:
    mode = (
        oracledb_module.AUTH_MODE_SYSDBA
        if os.environ["SANDBOX_ORACLE_USER"].lower() == "sys"
        else oracledb_module.AUTH_MODE_DEFAULT
    )
    return {
        "user": os.environ["SANDBOX_ORACLE_USER"],
        "password": os.environ["SANDBOX_ORACLE_PASSWORD"],
        "dsn": build_oracle_sandbox_dsn(),
        "mode": mode,
    }
```

- [ ] **Step 4: Update Oracle sandbox manifest helper**

In `tests/integration/runtime_helpers.py`, rewrite `build_oracle_sandbox_manifest()`:

```python
def build_oracle_sandbox_manifest() -> dict[str, object]:
    return {
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ["SOURCE_ORACLE_HOST"],
                    "port": os.environ.get("SOURCE_ORACLE_PORT", "1521"),
                    "service": os.environ["SOURCE_ORACLE_SERVICE"],
                    "user": os.environ["SOURCE_ORACLE_USER"],
                    "schema": ORACLE_MIGRATION_SCHEMA,
                    "password_env": "SOURCE_ORACLE_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ["SANDBOX_ORACLE_HOST"],
                    "port": os.environ.get("SANDBOX_ORACLE_PORT", "1521"),
                    "service": os.environ["SANDBOX_ORACLE_SERVICE"],
                    "user": os.environ["SANDBOX_ORACLE_USER"],
                    "password_env": "SANDBOX_ORACLE_PASSWORD",
                },
            },
        }
    }
```

- [ ] **Step 5: Update Oracle availability and materialization helpers**

In `tests/integration/runtime_helpers.py`, replace `oracle_is_available()`:

```python
def oracle_is_available(oracledb_module: Any) -> bool:
    if any(not os.environ.get(name) for name in ORACLE_SOURCE_ENV):
        return False
    try:
        conn = oracledb_module.connect(
            user=os.environ["SOURCE_ORACLE_USER"],
            password=os.environ["SOURCE_ORACLE_PASSWORD"],
            dsn=build_oracle_dsn(),
        )
        conn.close()
        return True
    except oracledb_module.Error:
        return False
```

Add:

```python
def oracle_sandbox_is_available(oracledb_module: Any) -> bool:
    if any(not os.environ.get(name) for name in ORACLE_SANDBOX_ENV):
        return False
    try:
        conn = oracledb_module.connect(
            **build_oracle_sandbox_admin_connect_kwargs(oracledb_module)
        )
        conn.close()
        return True
    except oracledb_module.Error:
        return False
```

Update `ensure_oracle_migration_test_materialized()`:

```python
def ensure_oracle_migration_test_materialized() -> None:
    global _ORACLE_MIGRATION_TEST_READY
    if _ORACLE_MIGRATION_TEST_READY:
        return

    require_env("source", ORACLE_SOURCE_ENV)
    require_env("sandbox", ORACLE_SANDBOX_ENV)
    pytest.importorskip(
        "oracledb",
        reason="oracledb not installed — required for Oracle materialization",
    )

    role = build_oracle_fixture_admin_role()
    result = materialize_migration_test(
        role,
        REPO_ROOT,
        extra_env={"ORACLE_SCHEMA_PASSWORD": os.environ["SOURCE_ORACLE_PASSWORD"]},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Oracle MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _ORACLE_MIGRATION_TEST_READY = True
```

- [ ] **Step 6: Update Oracle extract env fixture**

In `tests/integration/runtime_helpers.py`, update `configure_oracle_extract_env()`:

```python
def configure_oracle_extract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    ensure_oracle_migration_test_materialized()
    monkeypatch.setenv("SOURCE_ORACLE_USER", os.environ["SOURCE_ORACLE_USER"])
    monkeypatch.setenv("SOURCE_ORACLE_PASSWORD", os.environ["SOURCE_ORACLE_PASSWORD"])
    monkeypatch.setenv("ORACLE_DSN", build_oracle_dsn())
```

Update `require_oracle_extract_env()` so it checks:

```python
for var in ("SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD", "ORACLE_DSN"):
    if not os.environ.get(var):
        pytest.skip(f"source env missing: {var}")
```

Connect with:

```python
conn = oracledb.connect(
    user=os.environ["SOURCE_ORACLE_USER"],
    password=os.environ["SOURCE_ORACLE_PASSWORD"],
    dsn=os.environ["ORACLE_DSN"],
)
```

- [ ] **Step 7: Update Oracle integration tests**

Replace imports and calls:

```python
build_oracle_admin_connect_kwargs
```

with:

```python
build_oracle_sandbox_admin_connect_kwargs
```

Replace:

```python
build_oracle_admin_role
```

with:

```python
build_oracle_fixture_admin_role
```

Replace skip markers that use `oracle_is_available()` for sandbox lifecycle tests with `oracle_sandbox_is_available()`.

- [ ] **Step 8: Run Oracle unit and integration smoke**

Run:

```bash
cd lib && uv run pytest ../tests/integration/oracle/fixture_materialization -v
cd lib && uv run pytest ../tests/integration/oracle/setup_ddl -v
cd lib && uv run pytest ../tests/integration/oracle/test_harness -v
```

Expected: PASS when role-specific Oracle env is configured. Expected SKIP with a role-specific message when env is missing.

- [ ] **Step 9: Commit**

```bash
git add tests/integration/runtime_helpers.py tests/integration/oracle
git commit -m "Normalize Oracle integration role env"
```

---

### Task 6: Update Integration Documentation And Env Examples

**Files:**

- Modify: `.env.example`
- Modify: `.envrc`
- Modify: `docs/reference/setup-docker/README.md`
- Modify: `docs/wiki/SQL-Server-Connection-Variables.md`
- Modify: `docs/wiki/DDL-Extraction.md`
- Modify: `docs/wiki/Project-Init.md`
- Modify: `docs/design/init-ad-migration-prereqs/README.md`
- Modify: `docs/design/db-operations-api/README.md`

- [ ] **Step 1: Remove `MSSQL_DRIVER` docs and examples**

Search:

```bash
rg -n "MSSQL_DRIVER|ODBC Driver 18|ODBC Driver 17|driver override" .env.example .envrc docs
```

For customer/runtime docs, replace driver override guidance with:

```markdown
SQL Server connectivity uses FreeTDS. Install FreeTDS and unixODBC before running SQL Server source or sandbox commands.
```

- [ ] **Step 2: Update local Docker env examples**

In `docs/reference/setup-docker/README.md`, replace the SQL Server and Oracle env sample with:

```bash
# SQL Server source extraction role
SOURCE_MSSQL_HOST=127.0.0.1
SOURCE_MSSQL_PORT=1433
SOURCE_MSSQL_DB=AdventureWorks2022
SOURCE_MSSQL_USER=migrationtest_reader
SOURCE_MSSQL_PASSWORD=readonly-password

# SQL Server sandbox/admin role for local Docker
SANDBOX_MSSQL_HOST=127.0.0.1
SANDBOX_MSSQL_PORT=1433
SANDBOX_MSSQL_USER=sa
SANDBOX_MSSQL_PASSWORD=P@ssw0rd123

# Oracle source extraction role
SOURCE_ORACLE_HOST=localhost
SOURCE_ORACLE_PORT=1521
SOURCE_ORACLE_SERVICE=FREEPDB1
SOURCE_ORACLE_SCHEMA=MIGRATIONTEST
SOURCE_ORACLE_USER=MIGRATIONTEST
SOURCE_ORACLE_PASSWORD=migrationtest

# Oracle sandbox/admin role for local Docker
SANDBOX_ORACLE_HOST=localhost
SANDBOX_ORACLE_PORT=1521
SANDBOX_ORACLE_SERVICE=FREEPDB1
SANDBOX_ORACLE_USER=sys
SANDBOX_ORACLE_PASSWORD=P@ssw0rd123
```

Use example secret values only in documentation snippets. Do not commit real credentials.

- [ ] **Step 3: Update design docs to reference new integration contract**

In `docs/design/db-operations-api/README.md`, in the `MigrationTest Fixture` section, add:

```markdown
The role-specific source, sandbox, and target test contract is defined in [Integration Test Contract](../integration-test-contract/README.md).
```

Remove stale text implying a single env shape for fixture secrets.

- [ ] **Step 4: Run markdown lint**

Run:

```bash
markdownlint .env.example .envrc docs/reference/setup-docker/README.md docs/wiki/SQL-Server-Connection-Variables.md docs/wiki/DDL-Extraction.md docs/wiki/Project-Init.md docs/design/init-ad-migration-prereqs/README.md docs/design/db-operations-api/README.md docs/design/integration-test-contract/README.md
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .env.example .envrc docs/reference/setup-docker/README.md docs/wiki/SQL-Server-Connection-Variables.md docs/wiki/DDL-Extraction.md docs/wiki/Project-Init.md docs/design/init-ad-migration-prereqs/README.md docs/design/db-operations-api/README.md
git commit -m "Document role-specific integration env"
```

---

### Task 7: Run Final Verification

**Files:**

- Verify only; no planned edits.

- [ ] **Step 1: Run focused unit tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/fixture_materialization/test_fixture_materialization.py ../tests/unit/setup_ddl ../tests/unit/cli/test_sandbox_cmds.py -v
```

Expected: PASS.

- [ ] **Step 2: Run SQL Server integration tests when env is configured**

Run:

```bash
cd lib && uv run pytest ../tests/integration/sql_server/fixture_materialization ../tests/integration/sql_server/setup_ddl ../tests/integration/sql_server/catalog_diff -v
```

Expected with SQL Server role env configured: PASS. Expected without role env: SKIP with messages naming missing `source` or `sandbox` env vars.

- [ ] **Step 3: Run Oracle integration tests when env is configured**

Run:

```bash
cd lib && uv run pytest ../tests/integration/oracle/fixture_materialization ../tests/integration/oracle/setup_ddl ../tests/integration/oracle/test_harness -v
```

Expected with Oracle role env configured: PASS. Expected without role env: SKIP with messages naming missing `source` or `sandbox` env vars.

- [ ] **Step 4: Search for removed SQL Server driver contract**

Run:

```bash
rg -n "MSSQL_DRIVER|ODBC Driver 18|ODBC Driver 17|driver override" lib tests docs .env.example .envrc
```

Expected: no customer/runtime or integration-test references remain. Allowed references are historical plan files under `docs/superpowers/plans/` and generated eval fixtures if they are intentionally not part of this implementation.

- [ ] **Step 5: Search for legacy integration env conflation**

Run:

```bash
rg -n "SA_PASSWORD|MSSQL_HOST|MSSQL_USER|ORACLE_PWD|ORACLE_ADMIN_USER|ORACLE_SCHEMA_PASSWORD|ORACLE_USER|ORACLE_PASSWORD" tests/integration lib/shared/db_connect.py lib/shared/setup_ddl_support/manifest.py lib/shared/cli/setup_sandbox_cmd.py
```

Expected: no integration helper or runtime path uses legacy env names for source extraction. Local Docker docs may mention `sa` and `sys` only as sandbox/admin examples.

- [ ] **Step 6: Run markdown lint for changed docs**

Run:

```bash
markdownlint docs/design/integration-test-contract/README.md docs/design/db-operations-api/README.md docs/reference/setup-docker/README.md docs/wiki/SQL-Server-Connection-Variables.md docs/wiki/DDL-Extraction.md docs/wiki/Project-Init.md .env.example .envrc
```

Expected: PASS.

---

## Self-Review

- Spec coverage: Tasks cover role-specific source/sandbox/target env, no compatibility aliases, FreeTDS-only SQL Server runtime, fixed `MigrationTest` extraction, broad `list-schemas`, fixture materialization via sandbox/admin role, target setup exception, and new-platform guidance.
- Placeholder scan: The plan avoids deferred implementation language and uses concrete example values in documentation snippets.
- Type consistency: `RuntimeConnection`, `RuntimeRole`, `build_sql_server_connection_string()`, `materialize_migration_test()`, and runtime role names match the current codebase.
