"""Integration tests for OracleSandbox — requires local Oracle Docker.

Run with: cd lib && uv run pytest ../tests/integration/oracle/test_harness -v

Requires:
- Docker Oracle container running (see docs/reference/setup-docker/README.md)
- ORACLE_PWD env var set (SYS password)
- the Oracle materialize-migration-test script can connect to the container
"""

from __future__ import annotations

import os
import oracledb
import pytest

from shared.sandbox.oracle import OracleSandbox
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    ORACLE_MIGRATION_SCHEMA_PASSWORD,
    build_oracle_sandbox_manifest,
    ensure_oracle_migration_test_materialized,
    oracle_is_available,
)

pytestmark = pytest.mark.oracle

BRONZE_CURRENCY = "BRONZE_CURRENCY"
BRONZE_PROMOTION = "BRONZE_PROMOTION"
SILVER_DIMCURRENCY = "SILVER_DIMCURRENCY"
SILVER_DIMPROMOTION = "SILVER_DIMPROMOTION"
SILVER_CONFIG = "SILVER_CONFIG"
SILVER_VW_DIMPROMOTION = "SILVER_VW_DIMPROMOTION"
SILVER_USP_LOAD_DIMCURRENCY = "SILVER_USP_LOAD_DIMCURRENCY"
SILVER_USP_LOAD_DIMPROMOTION = "SILVER_USP_LOAD_DIMPROMOTION"
SILVER_USP_UNIONALL = "SILVER_USP_UNIONALL"


def _have_oracle_env() -> bool:
    return oracle_is_available(oracledb)


def _make_backend() -> OracleSandbox:
    ensure_oracle_migration_test_materialized()
    os.environ.setdefault("ORACLE_SCHEMA_PASSWORD", ORACLE_MIGRATION_SCHEMA_PASSWORD)
    return OracleSandbox.from_env(build_oracle_sandbox_manifest())


skip_no_oracle = pytest.mark.skipif(
    not _have_oracle_env(),
    reason="ORACLE_PWD not set — local Oracle Docker required",
)


@skip_no_oracle
class TestOracleSandboxLifecycle:
    """Full sandbox lifecycle: up → verify → down against local Oracle Docker."""

    def test_sandbox_up_creates_and_clones_migrationtest_schema(self) -> None:
        backend = _make_backend()

        try:
            result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = result.sandbox_database

            assert result.status in ("ok", "partial"), result.errors
            assert sandbox_schema.startswith("__test_")
            assert len(result.tables_cloned) > 0
            assert any(BRONZE_CURRENCY in t for t in result.tables_cloned)
            assert any(BRONZE_PROMOTION in t for t in result.tables_cloned)
            assert any(SILVER_DIMCURRENCY in t for t in result.tables_cloned)
            assert any(SILVER_DIMPROMOTION in t for t in result.tables_cloned)
            assert any(SILVER_CONFIG in t for t in result.tables_cloned)

            assert len(result.procedures_cloned) > 0
            assert any(SILVER_USP_LOAD_DIMCURRENCY in p for p in result.procedures_cloned)
            assert any(SILVER_USP_LOAD_DIMPROMOTION in p for p in result.procedures_cloned)
            assert any(SILVER_USP_UNIONALL in p for p in result.procedures_cloned)

            # Verify sandbox user exists and tables are accessible
            with backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1",
                    [sandbox_schema],
                )
                table_count = cursor.fetchone()[0]
                assert table_count > 0
        finally:
            backend.sandbox_down(sandbox_db=result.sandbox_database)

    def test_sandbox_down_removes_user(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_schema = result.sandbox_database
        down_result = backend.sandbox_down(sandbox_db=sandbox_schema)

        assert down_result.status == "ok"

        # Verify user is gone from ALL_USERS — no orphaned schema
        with backend._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                [sandbox_schema],
            )
            assert cursor.fetchone()[0] == 0, "Sandbox user should be dropped"

    def test_sandbox_status_reflects_existence(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_schema = result.sandbox_database

        try:
            status_up = backend.sandbox_status(sandbox_db=sandbox_schema)
            assert status_up.exists is True
            assert status_up.status == "ok"
        finally:
            backend.sandbox_down(sandbox_db=sandbox_schema)

        status_down = backend.sandbox_status(sandbox_db=sandbox_schema)
        assert status_down.exists is False
        assert status_down.status == "not_found"

    def test_sandbox_down_idempotent(self) -> None:
        backend = _make_backend()
        result = backend.sandbox_down(sandbox_db="__test_nonexistent99")
        assert result.status == "ok"


@skip_no_oracle
class TestOracleExecuteScenario:
    """Execute a real scenario against the sandbox using the MigrationTest schema."""

    def _create_temp_proc(self, backend: OracleSandbox, proc_name: str, body: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE OR REPLACE PROCEDURE {backend.source_schema}.{proc_name}
                AS
                BEGIN
                    {body}
                END;
                """
            )

    def _drop_temp_proc(self, backend: OracleSandbox, proc_name: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP PROCEDURE {backend.source_schema}.{proc_name}")
            except Exception:
                pass

    def test_full_lifecycle_execute_load_dimcurrency(self) -> None:
        """sandbox_up → execute_scenario against canonical currency tables → sandbox_down."""
        backend = _make_backend()
        proc_name = "PROC_LOAD_DIMCURRENCY"
        self._create_temp_proc(
            backend,
            proc_name,
            (
                f'DELETE FROM {SILVER_DIMCURRENCY}; '
                f'INSERT INTO {SILVER_DIMCURRENCY} ("CURRENCYKEY", "CURRENCYALTERNATEKEY", "CURRENCYNAME") '
                f'SELECT ROW_NUMBER() OVER (ORDER BY "CURRENCYCODE"), "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM {BRONZE_CURRENCY};'
            ),
        )

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database
            assert up_result.status in ("ok", "partial"), up_result.errors

            scenario = {
                "name": "test_load_dimcurrency",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": proc_name,
                "given": [
                    {
                        "table": BRONZE_CURRENCY,
                        "rows": [
                            {
                                "CURRENCYCODE": "USD",
                                "CURRENCYNAME": "US Dollar",
                                "MODIFIEDDATE": "2024-01-01",
                            },
                            {
                                "CURRENCYCODE": "EUR",
                                "CURRENCYNAME": "Euro",
                                "MODIFIEDDATE": "2024-01-02",
                            },
                        ],
                    },
                ],
            }

            result = backend.execute_scenario(
                sandbox_db=sandbox_schema, scenario=scenario,
            )

            assert result.status == "ok", result.errors
            assert result.scenario_name == "test_load_dimcurrency"
            assert result.row_count == 2
            assert result.errors == []

            rows = {r["CURRENCYALTERNATEKEY"]: r for r in result.ground_truth_rows}
            assert rows["USD"]["CURRENCYNAME"] == "US Dollar"
            assert rows["EUR"]["CURRENCYNAME"] == "Euro"

        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_temp_proc(backend, proc_name)

    def test_execute_rolls_back_fixture_data(self) -> None:
        """Fixture rows seeded during execute_scenario are rolled back."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            scenario = {
                "name": "test_rollback",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": SILVER_USP_LOAD_DIMCURRENCY,
                "given": [
                    {
                        "table": BRONZE_CURRENCY,
                        "rows": [
                            {
                                "CURRENCYCODE": "ZZZ",
                                "CURRENCYNAME": "Rollback Test",
                            },
                        ],
                    },
                ],
            }

            backend.execute_scenario(sandbox_db=sandbox_schema, scenario=scenario)

            # Verify fixture data was rolled back
            with backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{sandbox_schema}"."{BRONZE_CURRENCY}" '
                    f"WHERE \"CURRENCYCODE\" = 'ZZZ'"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_empty_fixtures(self) -> None:
        """Scenario with no fixture rows still runs the procedure (produces 0 rows)."""
        backend = _make_backend()
        proc_name = "PROC_EMPTY_FIXTURES"
        self._create_temp_proc(backend, proc_name, "NULL;")

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            scenario = {
                "name": "test_empty",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": proc_name,
                "given": [],
            }

            result = backend.execute_scenario(sandbox_db=sandbox_schema, scenario=scenario)
            assert result.status == "ok"
            assert result.row_count == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_temp_proc(backend, proc_name)


@skip_no_oracle
class TestOracleCompareTwoSql:
    """compare_two_sql against the sandbox."""

    def test_equivalent_selects_return_equivalent_true(self) -> None:
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            fixtures = [
                {
                    "table": BRONZE_CURRENCY,
                        "rows": [
                            {
                                "CURRENCYCODE": "USD",
                                "CURRENCYNAME": "US Dollar",
                                "MODIFIEDDATE": "2024-01-01",
                            },
                            {
                                "CURRENCYCODE": "EUR",
                                "CURRENCYNAME": "Euro",
                                "MODIFIEDDATE": "2024-01-02",
                            },
                        ],
                    },
            ]
            sql_a = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{BRONZE_CURRENCY}" '
                f'ORDER BY "CURRENCYCODE"'
            )
            sql_b = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{BRONZE_CURRENCY}" '
                f'ORDER BY "CURRENCYCODE"'
            )

            result = backend.compare_two_sql(
                sandbox_db=sandbox_schema,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )

            assert result["status"] == "ok"
            assert result["equivalent"] is True
            assert result["a_count"] == 2
            assert result["b_count"] == 2
            assert result["a_minus_b"] == []
            assert result["b_minus_a"] == []
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_non_equivalent_selects_return_equivalent_false(self) -> None:
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            fixtures = [
                {
                    "table": BRONZE_CURRENCY,
                        "rows": [
                            {
                                "CURRENCYCODE": "USD",
                                "CURRENCYNAME": "US Dollar",
                                "MODIFIEDDATE": "2024-01-01",
                            },
                            {
                                "CURRENCYCODE": "EUR",
                                "CURRENCYNAME": "Euro",
                                "MODIFIEDDATE": "2024-01-02",
                            },
                        ],
                    },
            ]
            sql_a = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{BRONZE_CURRENCY}"'
            )
            # Returns only one row
            sql_b = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{BRONZE_CURRENCY}" '
                f'WHERE "CURRENCYCODE" = \'USD\''
            )

            result = backend.compare_two_sql(
                sandbox_db=sandbox_schema,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )

            assert result["status"] == "ok"
            assert result["equivalent"] is False
            assert result["a_count"] == 2
            assert result["b_count"] == 1
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)


@skip_no_oracle
class TestOracleEnsureViewTablesIntegration:
    """Verify view-sourced fixtures are materialised end-to-end in a real Oracle sandbox."""

    def _create_view(self, backend: OracleSandbox, view_name: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            # Unquoted identifiers so ALL_VIEWS stores them without quotes
            cursor.execute(
                f"CREATE OR REPLACE VIEW {backend.source_schema}.{view_name} "
                f"AS SELECT CURRENCYCODE, CURRENCYNAME FROM {backend.source_schema}.{BRONZE_CURRENCY}"
            )

    def _drop_view(self, backend: OracleSandbox, view_name: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP VIEW {backend.source_schema}.{view_name}")
            except Exception:
                pass

    def _create_proc(
        self, backend: OracleSandbox, proc_name: str, view_name: str
    ) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            # Unquoted identifiers so ALL_SOURCE stores them without quotes,
            # allowing _clone_procedures' regex substitution to match.
            # Unqualified table names (SILVER_DIMCURRENCY, view_name) resolve
            # to the sandbox schema when the cloned procedure runs there.
            cursor.execute(
                f"""
                CREATE OR REPLACE PROCEDURE {backend.source_schema}.{proc_name}
                AS
                BEGIN
                    DELETE FROM {SILVER_DIMCURRENCY}
                    WHERE CURRENCYALTERNATEKEY IN (SELECT CURRENCYCODE FROM {view_name});
                END;
                """
            )

    def _drop_proc(self, backend: OracleSandbox, proc_name: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"DROP PROCEDURE {backend.source_schema}.{proc_name}"
                )
            except Exception:
                pass

    def test_view_fixture_executes_without_error(self) -> None:
        """execute_scenario succeeds when a fixture source is a view in the source schema."""
        import uuid as _uuid

        backend = _make_backend()
        hex_suffix = _uuid.uuid4().hex[:10].upper()
        view_name = f"VW_TEST_{hex_suffix}"
        proc_name = f"PROC_FROMVIEW_{hex_suffix}"

        self._create_view(backend, view_name)
        self._create_proc(backend, proc_name, view_name)

        up_result: dict = {}
        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database
            assert up_result.status in ("ok", "partial"), up_result.errors

            scenario = {
                "name": "test_view_fixture",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": proc_name,
                "given": [
                    {
                        "table": view_name,
                        "rows": [
                            {
                                "CURRENCYCODE": "VWT",
                                "CURRENCYNAME": "View Test",
                            },
                        ],
                    }
                ],
            }

            result = backend.execute_scenario(
                sandbox_db=sandbox_schema, scenario=scenario
            )

            assert result.status == "ok", result.errors
            assert result.errors == []
        finally:
            if up_result is not None:
                backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_proc(backend, proc_name)
            self._drop_view(backend, view_name)


@skip_no_oracle
class TestOracleSandboxNoOrphanedUsers:
    """Verify sandbox_down leaves no orphaned users in ALL_USERS."""

    def test_sandbox_down_leaves_no_orphaned_user(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_schema = result.sandbox_database
        assert result.status in ("ok", "partial")

        backend.sandbox_down(sandbox_db=sandbox_schema)

        with backend._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                [sandbox_schema],
            )
            count = cursor.fetchone()[0]
            assert count == 0, f"Orphaned user {sandbox_schema!r} found in ALL_USERS after teardown"


@skip_no_oracle
class TestOracleExecuteSelectIntegration:
    """execute_select against a real Oracle sandbox."""

    def test_execute_select_returns_fixture_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns correct rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database
            assert up_result.status in ("ok", "partial")

            fixtures = [
                {
                    "table": SILVER_DIMCURRENCY,
                        "rows": [
                        {
                            "CURRENCYKEY": 1,
                            "CURRENCYALTERNATEKEY": "USD",
                            "CURRENCYNAME": "US Dollar",
                        },
                        {
                            "CURRENCYKEY": 2,
                            "CURRENCYALTERNATEKEY": "EUR",
                            "CURRENCYNAME": "Euro",
                        },
                    ],
                },
            ]
            sql = (
                f'SELECT "CURRENCYALTERNATEKEY", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}" '
                f'ORDER BY "CURRENCYALTERNATEKEY"'
            )

            result = backend.execute_select(
                sandbox_db=sandbox_schema, sql=sql, fixtures=fixtures,
            )

            assert result.status == "ok", result.errors
            assert result.row_count == 2
            assert result.errors == []
            rows = result.ground_truth_rows
            codes = {r["CURRENCYALTERNATEKEY"] for r in rows}
            assert codes == {"USD", "EUR"}
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_compare_two_sql_returns_equivalent_for_same_result_set(self) -> None:
        """compare_two_sql reports equivalent when both SQLs return the same rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database
            fixtures = [
                {
                    "table": SILVER_DIMCURRENCY,
                        "rows": [
                        {
                            "CURRENCYKEY": 1,
                            "CURRENCYALTERNATEKEY": "USD",
                            "CURRENCYNAME": "US Dollar",
                        },
                        {
                            "CURRENCYKEY": 2,
                            "CURRENCYALTERNATEKEY": "EUR",
                            "CURRENCYNAME": "Euro",
                        },
                    ],
                },
            ]
            sql_a = (
                f'SELECT "CURRENCYALTERNATEKEY", "CURRENCYNAME" '
                f'FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}"'
            )
            sql_b = (
                "WITH src AS ("
                f'  SELECT "CURRENCYALTERNATEKEY", "CURRENCYNAME" FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}"'
                ") "
                'SELECT "CURRENCYALTERNATEKEY", "CURRENCYNAME" FROM src'
            )

            result = backend.compare_two_sql(
                sandbox_db=sandbox_schema,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )

            assert result["status"] == "ok", result["errors"]
            assert result["equivalent"] is True
            assert result["a_minus_b"] == []
            assert result["b_minus_a"] == []
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_select_empty_fixtures(self) -> None:
        """execute_select with no fixture rows returns 0 rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            result = backend.execute_select(
                sandbox_db=sandbox_schema,
                sql=f'SELECT "CURRENCYALTERNATEKEY" FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}"',
                fixtures=[],
            )

            assert result.status == "ok"
            assert result.row_count == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_select_rolls_back_fixtures(self) -> None:
        """Fixture rows are rolled back after execute_select."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            fixtures = [
                {
                    "table": SILVER_DIMCURRENCY,
                    "rows": [
                        {
                            "CURRENCYALTERNATEKEY": "ZZZ",
                            "CURRENCYNAME": "Rollback Test",
                        },
                    ],
                },
            ]
            backend.execute_select(
                sandbox_db=sandbox_schema,
                sql=f'SELECT "CURRENCYALTERNATEKEY" FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}"',
                fixtures=fixtures,
            )

            # Verify fixture row was rolled back
            with backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{sandbox_schema}"."{SILVER_DIMCURRENCY}" '
                    f"WHERE \"CURRENCYALTERNATEKEY\" = 'ZZZ'"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
