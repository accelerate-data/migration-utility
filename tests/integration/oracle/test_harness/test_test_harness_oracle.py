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

BRONZE_CURRENCY = f"{ORACLE_MIGRATION_SCHEMA}.BRONZE_CURRENCY"
BRONZE_PROMOTION = f"{ORACLE_MIGRATION_SCHEMA}.BRONZE_PROMOTION"
SILVER_DIMCURRENCY = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_DIMCURRENCY"
SILVER_DIMPROMOTION = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_DIMPROMOTION"
SILVER_CONFIG = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_CONFIG"
SILVER_VW_DIMPROMOTION = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_VW_DIMPROMOTION"
SILVER_USP_LOAD_DIMCURRENCY = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_USP_LOAD_DIMCURRENCY"
SILVER_USP_LOAD_DIMPROMOTION = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_USP_LOAD_DIMPROMOTION"
SILVER_USP_UNIONALL = f"{ORACLE_MIGRATION_SCHEMA}.SILVER_USP_UNIONALL"


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
            assert sandbox_schema.startswith("SBX_")
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

            # Verify sandbox PDB exists and tables are accessible
            with backend._connect_sandbox(sandbox_schema) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1",
                    [ORACLE_MIGRATION_SCHEMA],
                )
                table_count = cursor.fetchone()[0]
                assert table_count > 0
        finally:
            backend.sandbox_down(sandbox_db=result.sandbox_database)

    def test_sandbox_down_removes_pdb(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_schema = result.sandbox_database
        down_result = backend.sandbox_down(sandbox_db=sandbox_schema)

        assert down_result.status == "ok"

        # Verify PDB is gone from V$PDBS — no orphaned sandbox
        with backend._connect_cdb() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM V$PDBS WHERE NAME = UPPER(:1)",
                [sandbox_schema],
            )
            assert cursor.fetchone()[0] == 0, "Sandbox PDB should be dropped"

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
        result = backend.sandbox_down(sandbox_db="SBX_000000000099")
        assert result.status == "ok"


@skip_no_oracle
class TestOracleExecuteScenario:
    """Execute a real scenario against the sandbox using the MigrationTest schema."""

    def _create_temp_proc(self, backend: OracleSandbox, proc_name: str, body: str) -> None:
        with backend._connect_source() as conn:
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
        with backend._connect_source() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP PROCEDURE {backend.source_schema}.{proc_name}")
            except Exception:
                pass

    def test_full_lifecycle_execute_load_dimcurrency(self) -> None:
        """sandbox_up → execute_scenario against canonical currency tables → sandbox_down."""
        backend = _make_backend()
        bare_proc = "PROC_LOAD_DIMCURRENCY"
        self._create_temp_proc(
            backend,
            bare_proc,
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
                "procedure": f"{ORACLE_MIGRATION_SCHEMA}.{bare_proc}",
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
            self._drop_temp_proc(backend, bare_proc)

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
                                "MODIFIEDDATE": "2024-01-03",
                            },
                        ],
                    },
                ],
            }

            backend.execute_scenario(sandbox_db=sandbox_schema, scenario=scenario)

            # Verify fixture data was rolled back
            with backend._connect_sandbox(sandbox_schema) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{ORACLE_MIGRATION_SCHEMA}"."BRONZE_CURRENCY" '
                    f"WHERE \"CURRENCYCODE\" = 'ZZZ'"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)

    def test_execute_empty_fixtures(self) -> None:
        """Scenario with no fixture rows still runs the procedure (produces 0 rows)."""
        backend = _make_backend()
        bare_proc = "PROC_EMPTY_FIXTURES"
        self._create_temp_proc(backend, bare_proc, "NULL;")

        try:
            up_result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
            sandbox_schema = up_result.sandbox_database

            scenario = {
                "name": "test_empty",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": f"{ORACLE_MIGRATION_SCHEMA}.{bare_proc}",
                "given": [],
            }

            result = backend.execute_scenario(sandbox_db=sandbox_schema, scenario=scenario)
            assert result.status == "ok"
            assert result.row_count == 0
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
            self._drop_temp_proc(backend, bare_proc)


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
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."BRONZE_CURRENCY" '
                f'ORDER BY "CURRENCYCODE"'
            )
            sql_b = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."BRONZE_CURRENCY" '
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
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."BRONZE_CURRENCY"'
            )
            # Returns only one row
            sql_b = (
                f'SELECT "CURRENCYCODE", "CURRENCYNAME" '
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."BRONZE_CURRENCY" '
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
        with backend._connect_source() as conn:
            cursor = conn.cursor()
            # Unquoted identifiers so ALL_VIEWS stores them without quotes
            cursor.execute(
                f"CREATE OR REPLACE VIEW {backend.source_schema}.{view_name} "
                f"AS SELECT CURRENCYCODE, CURRENCYNAME FROM {BRONZE_CURRENCY}"
            )

    def _drop_view(self, backend: OracleSandbox, view_name: str) -> None:
        with backend._connect_source() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP VIEW {backend.source_schema}.{view_name}")
            except Exception:
                pass

    def _create_proc(
        self, backend: OracleSandbox, proc_name: str, view_name: str
    ) -> None:
        with backend._connect_source() as conn:
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
        with backend._connect_source() as conn:
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

            qualified_view = f"{ORACLE_MIGRATION_SCHEMA}.{view_name}"
            qualified_proc = f"{ORACLE_MIGRATION_SCHEMA}.{proc_name}"

            scenario = {
                "name": "test_view_fixture",
                "target_table": SILVER_DIMCURRENCY,
                "procedure": qualified_proc,
                "given": [
                    {
                        "table": qualified_view,
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
class TestOracleSandboxNoOrphanedPdbs:
    """Verify sandbox_down leaves no orphaned PDBs in V$PDBS."""

    def test_sandbox_down_leaves_no_orphaned_pdb(self) -> None:
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_schema = result.sandbox_database
        assert result.status in ("ok", "partial")

        backend.sandbox_down(sandbox_db=sandbox_schema)

        with backend._connect_cdb() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM V$PDBS WHERE NAME = UPPER(:1)",
                [sandbox_schema],
            )
            count = cursor.fetchone()[0]
            assert count == 0, f"Orphaned PDB {sandbox_schema!r} found in V$PDBS after teardown"


@skip_no_oracle
class TestOraclePdbLifecycle:
    """Low-level PDB create/drop/connect tests against local Oracle Docker."""

    def test_create_sandbox_pdb_creates_and_opens_pdb(self) -> None:
        """_create_sandbox_pdb registers PDB in V$PDBS with OPEN status."""
        backend = _make_backend()
        from shared.sandbox.base import generate_sandbox_name

        sandbox_name = generate_sandbox_name()
        try:
            backend._create_sandbox_pdb(sandbox_name)

            with backend._connect_cdb() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT OPEN_MODE FROM V$PDBS WHERE NAME = UPPER(:1)",
                    [sandbox_name],
                )
                row = cursor.fetchone()
                assert row is not None, f"PDB {sandbox_name!r} not found in V$PDBS"
                assert row[0].startswith("READ WRITE"), (
                    f"Expected OPEN status, got {row[0]!r}"
                )
        finally:
            backend._drop_sandbox_pdb(sandbox_name)

    def test_drop_sandbox_pdb_removes_pdb(self) -> None:
        """_drop_sandbox_pdb removes PDB from V$PDBS."""
        backend = _make_backend()
        from shared.sandbox.base import generate_sandbox_name

        sandbox_name = generate_sandbox_name()
        backend._create_sandbox_pdb(sandbox_name)
        backend._drop_sandbox_pdb(sandbox_name)

        with backend._connect_cdb() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM V$PDBS WHERE NAME = UPPER(:1)",
                [sandbox_name],
            )
            assert cursor.fetchone()[0] == 0, (
                f"PDB {sandbox_name!r} should be gone from V$PDBS"
            )

    def test_drop_sandbox_pdb_idempotent_for_nonexistent(self) -> None:
        """_drop_sandbox_pdb does not raise for a PDB that never existed."""
        backend = _make_backend()
        # Should not raise — silently ignores missing PDBs
        backend._drop_sandbox_pdb("SBX_000000000099")

    def test_connect_sandbox_connects_to_pdb(self) -> None:
        """_connect_sandbox opens a usable connection to a sandbox PDB."""
        backend = _make_backend()
        from shared.sandbox.base import generate_sandbox_name

        sandbox_name = generate_sandbox_name()
        try:
            backend._create_sandbox_pdb(sandbox_name)

            with backend._connect_sandbox(sandbox_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                assert cursor.fetchone()[0] == 1
        finally:
            backend._drop_sandbox_pdb(sandbox_name)

    def test_sandbox_up_creates_pdb_with_schema_objects(self) -> None:
        """Full sandbox_up creates a PDB visible in V$PDBS with cloned objects."""
        backend = _make_backend()

        result = backend.sandbox_up(schemas=[ORACLE_MIGRATION_SCHEMA])
        sandbox_name = result.sandbox_database
        try:
            assert result.status in ("ok", "partial"), result.errors
            assert sandbox_name.startswith("SBX_")

            # Verify PDB exists in V$PDBS
            with backend._connect_cdb() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT OPEN_MODE FROM V$PDBS WHERE NAME = UPPER(:1)",
                    [sandbox_name],
                )
                row = cursor.fetchone()
                assert row is not None, f"PDB {sandbox_name!r} not found in V$PDBS"
                assert row[0].startswith("READ WRITE")

            # Verify schema objects are accessible inside the PDB
            with backend._connect_sandbox(sandbox_name) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1",
                    [ORACLE_MIGRATION_SCHEMA],
                )
                table_count = cursor.fetchone()[0]
                assert table_count > 0, "Expected cloned tables in sandbox PDB"
        finally:
            backend.sandbox_down(sandbox_db=sandbox_name)


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
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY" '
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
                f'FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY"'
            )
            sql_b = (
                "WITH src AS ("
                f'  SELECT "CURRENCYALTERNATEKEY", "CURRENCYNAME" FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY"'
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
                sql=f'SELECT "CURRENCYALTERNATEKEY" FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY"',
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
                sql=f'SELECT "CURRENCYALTERNATEKEY" FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY"',
                fixtures=fixtures,
            )

            # Verify fixture row was rolled back
            with backend._connect_sandbox(sandbox_schema) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{ORACLE_MIGRATION_SCHEMA}"."SILVER_DIMCURRENCY" '
                    f"WHERE \"CURRENCYALTERNATEKEY\" = 'ZZZ'"
                )
                assert cursor.fetchone()[0] == 0, "Fixture row should be rolled back"
        finally:
            backend.sandbox_down(sandbox_db=up_result.sandbox_database)
