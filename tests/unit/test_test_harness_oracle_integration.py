"""Integration tests for OracleSandbox — requires local Oracle Docker with SH schema.

Run with: cd plugin/lib && uv run pytest -m oracle -v

Requires:
- Docker Oracle container running (see docs/reference/setup-docker/README.md)
- ORACLE_PWD env var set (SYS password)
- SH schema loaded with CHANNEL_SALES_SUMMARY table and SUMMARIZE_CHANNEL_SALES
  procedure (scripts/sql/oracle/synthetic_sales_procedure.sql)
"""

from __future__ import annotations

import os

import pytest

from shared.sandbox.oracle import OracleSandbox

pytestmark = pytest.mark.oracle


def _have_oracle_env() -> bool:
    return bool(os.environ.get("ORACLE_PWD"))


def _make_backend() -> OracleSandbox:
    return OracleSandbox(
        host=os.environ.get("ORACLE_HOST", "localhost"),
        port=os.environ.get("ORACLE_PORT", "1521"),
        service=os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
        password=os.environ.get("ORACLE_PWD", ""),
        admin_user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
        source_schema="SH",
    )


skip_no_oracle = pytest.mark.skipif(
    not _have_oracle_env(),
    reason="ORACLE_PWD not set — local Oracle Docker required",
)


@skip_no_oracle
class TestOracleSandboxLifecycle:
    """Full sandbox lifecycle: up → verify → down against local Oracle Docker."""

    def test_sandbox_up_creates_and_clones_sh_schema(self) -> None:
        backend = _make_backend()

        try:
            result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = result["sandbox_database"]

            assert result["status"] in ("ok", "partial"), result["errors"]
            assert sandbox_schema.startswith("__test_")
            assert len(result["tables_cloned"]) > 0
            assert any("CHANNELS" in t for t in result["tables_cloned"])
            assert any("SALES" in t for t in result["tables_cloned"])
            assert any("CHANNEL_SALES_SUMMARY" in t for t in result["tables_cloned"])

            assert len(result["procedures_cloned"]) > 0
            assert any("SUMMARIZE_CHANNEL_SALES" in p for p in result["procedures_cloned"])

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
            backend.sandbox_down(sandbox_db=result["sandbox_database"])

    def test_sandbox_down_removes_user(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=["SH"])
        sandbox_schema = result["sandbox_database"]
        down_result = backend.sandbox_down(sandbox_db=sandbox_schema)

        assert down_result["status"] == "ok"

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

        result = backend.sandbox_up(schemas=["SH"])
        sandbox_schema = result["sandbox_database"]

        try:
            status_up = backend.sandbox_status(sandbox_db=sandbox_schema)
            assert status_up["exists"] is True
            assert status_up["status"] == "ok"
        finally:
            backend.sandbox_down(sandbox_db=sandbox_schema)

        status_down = backend.sandbox_status(sandbox_db=sandbox_schema)
        assert status_down["exists"] is False
        assert status_down["status"] == "not_found"

    def test_sandbox_down_idempotent(self) -> None:
        backend = _make_backend()
        result = backend.sandbox_down(sandbox_db="__test_nonexistent99")
        assert result["status"] == "ok"


@skip_no_oracle
class TestOracleExecuteScenario:
    """Execute a real scenario against the sandbox using the SH schema."""

    def test_full_lifecycle_execute_summarize_channel_sales(self) -> None:
        """sandbox_up → execute_scenario (SUMMARIZE_CHANNEL_SALES) → sandbox_down."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]
            assert up_result["status"] in ("ok", "partial"), up_result["errors"]

            scenario = {
                "name": "test_summarize_channel_sales",
                "target_table": "CHANNEL_SALES_SUMMARY",
                "procedure": "SUMMARIZE_CHANNEL_SALES",
                "given": [
                    {
                        "table": "CHANNELS",
                        "rows": [
                            {
                                "CHANNEL_ID": 1,
                                "CHANNEL_DESC": "Direct Sales",
                                "CHANNEL_CLASS": "Direct",
                                "CHANNEL_CLASS_ID": 12,
                                "CHANNEL_TOTAL": "Channel total",
                                "CHANNEL_TOTAL_ID": 1,
                            },
                            {
                                "CHANNEL_ID": 2,
                                "CHANNEL_DESC": "Internet",
                                "CHANNEL_CLASS": "Indirect",
                                "CHANNEL_CLASS_ID": 13,
                                "CHANNEL_TOTAL": "Channel total",
                                "CHANNEL_TOTAL_ID": 1,
                            },
                        ],
                    },
                    {
                        "table": "SALES",
                        "rows": [
                            {
                                "PROD_ID": 1,
                                "CUST_ID": 1,
                                "TIME_ID": "1998-01-01",
                                "CHANNEL_ID": 1,
                                "PROMO_ID": 999,
                                "QUANTITY_SOLD": 10,
                                "AMOUNT_SOLD": 600000,
                            },
                            {
                                "PROD_ID": 2,
                                "CUST_ID": 2,
                                "TIME_ID": "1998-01-02",
                                "CHANNEL_ID": 2,
                                "PROMO_ID": 999,
                                "QUANTITY_SOLD": 5,
                                "AMOUNT_SOLD": 50000,
                            },
                        ],
                    },
                ],
            }

            result = backend.execute_scenario(
                sandbox_db=sandbox_schema, scenario=scenario,
            )

            assert result["status"] == "ok", result["errors"]
            assert result["scenario_name"] == "test_summarize_channel_sales"
            assert result["row_count"] == 2
            assert result["errors"] == []

            rows = {r["CHANNEL_ID"]: r for r in result["ground_truth_rows"]}
            # Channel 1 had 600000 → HIGH tier
            assert rows[1]["TIER"] == "HIGH"
            # Channel 2 had 50000 → LOW tier
            assert rows[2]["TIER"] == "LOW"

        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_execute_rolls_back_fixture_data(self) -> None:
        """Fixture rows seeded during execute_scenario are rolled back."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            scenario = {
                "name": "test_rollback",
                "target_table": "CHANNEL_SALES_SUMMARY",
                "procedure": "SUMMARIZE_CHANNEL_SALES",
                "given": [
                    {
                        "table": "CHANNELS",
                        "rows": [
                            {
                                "CHANNEL_ID": 99,
                                "CHANNEL_DESC": "Test",
                                "CHANNEL_CLASS": "Test",
                                "CHANNEL_CLASS_ID": 99,
                                "CHANNEL_TOTAL": "Channel total",
                                "CHANNEL_TOTAL_ID": 1,
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
                    f'SELECT COUNT(*) FROM "{sandbox_schema}".CHANNELS '
                    f"WHERE CHANNEL_ID = 99"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_execute_empty_fixtures(self) -> None:
        """Scenario with no fixture rows still runs the procedure (produces 0 rows)."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            scenario = {
                "name": "test_empty",
                "target_table": "CHANNEL_SALES_SUMMARY",
                "procedure": "SUMMARIZE_CHANNEL_SALES",
                "given": [],
            }

            result = backend.execute_scenario(sandbox_db=sandbox_schema, scenario=scenario)
            assert result["status"] == "ok"
            assert result["row_count"] == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])


@skip_no_oracle
class TestOracleCompareTwoSql:
    """compare_two_sql against the sandbox."""

    def test_equivalent_selects_return_equivalent_true(self) -> None:
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            fixtures = [
                {
                    "table": "CHANNELS",
                    "rows": [
                        {
                            "CHANNEL_ID": 1,
                            "CHANNEL_DESC": "Direct",
                            "CHANNEL_CLASS": "Direct",
                            "CHANNEL_CLASS_ID": 12,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                        {
                            "CHANNEL_ID": 2,
                            "CHANNEL_DESC": "Internet",
                            "CHANNEL_CLASS": "Indirect",
                            "CHANNEL_CLASS_ID": 13,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                    ],
                },
            ]
            sql_a = (
                f'SELECT CHANNEL_ID, CHANNEL_DESC '
                f'FROM "{sandbox_schema}"."CHANNELS" '
                f'ORDER BY CHANNEL_ID'
            )
            sql_b = (
                f'SELECT CHANNEL_ID, CHANNEL_DESC '
                f'FROM "{sandbox_schema}"."CHANNELS" '
                f'ORDER BY CHANNEL_ID'
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
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_non_equivalent_selects_return_equivalent_false(self) -> None:
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            fixtures = [
                {
                    "table": "CHANNELS",
                    "rows": [
                        {
                            "CHANNEL_ID": 1,
                            "CHANNEL_DESC": "Direct",
                            "CHANNEL_CLASS": "Direct",
                            "CHANNEL_CLASS_ID": 12,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                        {
                            "CHANNEL_ID": 2,
                            "CHANNEL_DESC": "Internet",
                            "CHANNEL_CLASS": "Indirect",
                            "CHANNEL_CLASS_ID": 13,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                    ],
                },
            ]
            sql_a = (
                f'SELECT CHANNEL_ID, CHANNEL_DESC '
                f'FROM "{sandbox_schema}"."CHANNELS"'
            )
            # Returns only one row
            sql_b = (
                f'SELECT CHANNEL_ID, CHANNEL_DESC '
                f'FROM "{sandbox_schema}"."CHANNELS" '
                f'WHERE CHANNEL_ID = 1'
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
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])


@skip_no_oracle
class TestOracleEnsureViewTablesIntegration:
    """Verify view-sourced fixtures are materialised end-to-end in a real Oracle sandbox."""

    def _create_view(self, backend: OracleSandbox, view_name: str) -> None:
        with backend._connect() as conn:
            cursor = conn.cursor()
            # Unquoted identifiers so ALL_VIEWS stores them without quotes
            cursor.execute(
                f"CREATE OR REPLACE VIEW {backend.source_schema}.{view_name} "
                f"AS SELECT CHANNEL_ID, CHANNEL_DESC FROM {backend.source_schema}.CHANNELS"
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
            # Unqualified table names (CHANNEL_SALES_SUMMARY, view_name) resolve
            # to the sandbox schema when the cloned procedure runs there.
            cursor.execute(
                f"""
                CREATE OR REPLACE PROCEDURE {backend.source_schema}.{proc_name}
                AS
                BEGIN
                    DELETE FROM CHANNEL_SALES_SUMMARY
                    WHERE CHANNEL_ID IN (SELECT CHANNEL_ID FROM {view_name});
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
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]
            assert up_result["status"] in ("ok", "partial"), up_result["errors"]

            scenario = {
                "name": "test_view_fixture",
                "target_table": "CHANNEL_SALES_SUMMARY",
                "procedure": proc_name,
                "given": [
                    {
                        "table": view_name,
                        "rows": [
                            {
                                "CHANNEL_ID": 99,
                                "CHANNEL_DESC": "View Test",
                            },
                        ],
                    }
                ],
            }

            result = backend.execute_scenario(
                sandbox_db=sandbox_schema, scenario=scenario
            )

            assert result["status"] == "ok", result["errors"]
            assert result["errors"] == []
        finally:
            if up_result.get("sandbox_database"):
                backend.sandbox_down(sandbox_db=up_result["sandbox_database"])
            self._drop_proc(backend, proc_name)
            self._drop_view(backend, view_name)


@skip_no_oracle
class TestOracleSandboxNoOrphanedUsers:
    """Verify sandbox_down leaves no orphaned users in ALL_USERS."""

    def test_sandbox_down_leaves_no_orphaned_user(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=["SH"])
        sandbox_schema = result["sandbox_database"]
        assert result["status"] in ("ok", "partial")

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
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]
            assert up_result["status"] in ("ok", "partial")

            fixtures = [
                {
                    "table": "CHANNELS",
                    "rows": [
                        {
                            "CHANNEL_ID": 1,
                            "CHANNEL_DESC": "Direct",
                            "CHANNEL_CLASS": "Direct",
                            "CHANNEL_CLASS_ID": 12,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                        {
                            "CHANNEL_ID": 2,
                            "CHANNEL_DESC": "Internet",
                            "CHANNEL_CLASS": "Indirect",
                            "CHANNEL_CLASS_ID": 13,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                    ],
                },
            ]
            sql = (
                f'SELECT "CHANNEL_ID", "CHANNEL_DESC" '
                f'FROM "{sandbox_schema}"."CHANNELS" '
                f'ORDER BY "CHANNEL_ID"'
            )

            result = backend.execute_select(
                sandbox_db=sandbox_schema, sql=sql, fixtures=fixtures,
            )

            assert result["status"] == "ok", result["errors"]
            assert result["row_count"] == 2
            assert result["errors"] == []
            rows = result["ground_truth_rows"]
            descs = {r["CHANNEL_DESC"] for r in rows}
            assert descs == {"Direct", "Internet"}
        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_compare_two_sql_returns_equivalent_for_same_result_set(self) -> None:
        """compare_two_sql reports equivalent when both SQLs return the same rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]
            fixtures = [
                {
                    "table": "CHANNELS",
                    "rows": [
                        {
                            "CHANNEL_ID": 1,
                            "CHANNEL_DESC": "Direct",
                            "CHANNEL_CLASS": "Direct",
                            "CHANNEL_CLASS_ID": 12,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                        {
                            "CHANNEL_ID": 2,
                            "CHANNEL_DESC": "Internet",
                            "CHANNEL_CLASS": "Indirect",
                            "CHANNEL_CLASS_ID": 13,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                    ],
                },
            ]
            sql_a = (
                f'SELECT "CHANNEL_ID", "CHANNEL_DESC" '
                f'FROM "{sandbox_schema}"."CHANNELS"'
            )
            sql_b = (
                "WITH src AS ("
                f'  SELECT "CHANNEL_ID", "CHANNEL_DESC" FROM "{sandbox_schema}"."CHANNELS"'
                ") "
                'SELECT "CHANNEL_ID", "CHANNEL_DESC" FROM src'
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
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_execute_select_empty_fixtures(self) -> None:
        """execute_select with no fixture rows returns 0 rows."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            result = backend.execute_select(
                sandbox_db=sandbox_schema,
                sql=f'SELECT "CHANNEL_ID" FROM "{sandbox_schema}"."CHANNELS"',
                fixtures=[],
            )

            assert result["status"] == "ok"
            assert result["row_count"] == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])

    def test_execute_select_rolls_back_fixtures(self) -> None:
        """Fixture rows are rolled back after execute_select."""
        backend = _make_backend()

        try:
            up_result = backend.sandbox_up(schemas=["SH"])
            sandbox_schema = up_result["sandbox_database"]

            fixtures = [
                {
                    "table": "CHANNELS",
                    "rows": [
                        {
                            "CHANNEL_ID": 99,
                            "CHANNEL_DESC": "Rollback Test",
                            "CHANNEL_CLASS": "Test",
                            "CHANNEL_CLASS_ID": 99,
                            "CHANNEL_TOTAL": "Channel total",
                            "CHANNEL_TOTAL_ID": 1,
                        },
                    ],
                },
            ]
            backend.execute_select(
                sandbox_db=sandbox_schema,
                sql=f'SELECT "CHANNEL_ID" FROM "{sandbox_schema}"."CHANNELS"',
                fixtures=fixtures,
            )

            # Verify fixture row was rolled back
            with backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{sandbox_schema}"."CHANNELS" '
                    f"WHERE \"CHANNEL_ID\" = 99"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result["sandbox_database"])
