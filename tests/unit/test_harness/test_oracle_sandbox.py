"""Oracle-specific validation, facade execution, and comparison tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox.base import generate_sandbox_name
from shared.sandbox.oracle import (
    OracleSandbox,
    _generate_oracle_pdb_name,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
)
from shared.sandbox.oracle_services import (
    _parse_qualified_name,
    _validate_oracle_qualified_name,
)


# ── Oracle identifier validation ──────────────────────────────────────────────


class TestOracleIdentifierValidation:
    def test_simple_name(self) -> None:
        _validate_oracle_identifier("CHANNELS")  # should not raise

    def test_underscore_prefix(self) -> None:
        _validate_oracle_identifier("_MY_TABLE")

    def test_dollar_sign(self) -> None:
        _validate_oracle_identifier("SYS$TABLE")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("")

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("TABLE; DROP TABLE CHANNELS--")

    def test_rejects_single_quote(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("O'REILLY")

    def test_rejects_double_quote(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier('"quoted"')

    def test_rejects_over_128_chars(self) -> None:
        long_name = "A" * 129
        with pytest.raises(ValueError, match="exceeds 128 chars"):
            _validate_oracle_identifier(long_name)

    def test_accepts_128_chars(self) -> None:
        name = "A" * 128
        _validate_oracle_identifier(name)  # should not raise

    def test_rejects_dot(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("SH.CHANNELS")


# ── Oracle sandbox name generation + validation ───────────────────────────────


class TestOracleSandboxName:
    def test_pdb_name_has_correct_prefix(self) -> None:
        name = _generate_oracle_pdb_name()
        assert name.startswith("SBX_")

    def test_pdb_name_is_unique(self) -> None:
        names = {_generate_oracle_pdb_name() for _ in range(10)}
        assert len(names) == 10

    def test_pdb_name_passes_validation(self) -> None:
        name = _generate_oracle_pdb_name()
        _validate_oracle_sandbox_name(name)  # should not raise

    def test_pdb_name_hex_length(self) -> None:
        name = _generate_oracle_pdb_name()
        hex_part = name[len("SBX_"):]
        assert len(hex_part) == 12
        assert all(c in "0123456789ABCDEF" for c in hex_part)

    def test_pdb_name_is_uppercase(self) -> None:
        name = _generate_oracle_pdb_name()
        assert name == name.upper()

    def test_base_generate_sandbox_name_passes_validation(self) -> None:
        """Base generate_sandbox_name() now produces SBX_ names that pass validation."""
        name = generate_sandbox_name()
        _validate_oracle_sandbox_name(name)  # should not raise

    def test_rejects_name_without_prefix(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("myschema")

    def test_rejects_legacy_test_prefix(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("__test_abc123abcd")

    def test_rejects_name_with_special_chars(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("SBX_ABC; DROP")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("")


class TestOracleFacadeDelegation:
    def test_public_execution_methods_delegate_to_execution_service(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        backend._execution = MagicMock()
        backend._execution.execute_scenario.return_value = "scenario-result"
        backend._execution.execute_select.return_value = "select-result"
        backend._comparison = MagicMock()
        backend._comparison.compare_two_sql.return_value = "compare-result"

        scenario = {
            "name": "case",
            "target_table": "SH.CHANNELS",
            "procedure": "SH.LOAD_CHANNELS",
            "given": [],
        }
        fixtures: list[dict[str, object]] = []

        assert backend.execute_scenario("SBX_000000000001", scenario) == "scenario-result"
        assert backend.execute_select("SBX_000000000001", "SELECT 1 FROM dual", fixtures) == "select-result"
        assert backend.compare_two_sql(
            "SBX_000000000001", "SELECT 1 FROM dual", "SELECT 1 FROM dual", fixtures,
        ) == "compare-result"

        backend._execution.execute_scenario.assert_called_once_with(
            "SBX_000000000001", scenario,
        )
        backend._execution.execute_select.assert_called_once_with(
            "SBX_000000000001", "SELECT 1 FROM dual", fixtures,
        )
        backend._comparison.compare_two_sql.assert_called_once_with(
            "SBX_000000000001", "SELECT 1 FROM dual", "SELECT 1 FROM dual", fixtures,
        )


class TestExecuteScenarioOracle:
    def test_execute_scenario_quotes_procedure_name(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cursor = MagicMock()
        cursor.description = [("ID",)]
        cursor.fetchall.return_value = [(1,)]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_sandbox(name: str):
            yield conn

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox), \
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]), \
             patch.object(backend._fixtures, "seed_fixtures"):
            result = backend.execute_scenario(
                sandbox_db="SBX_ABC123000000",
                scenario={
                    "name": "quoted_proc",
                    "procedure": "SH.Proc$Load",
                    "target_table": "SH.CHANNELS",
                    "given": [],
                },
            )

        assert result.status == "ok"
        execute_calls = [call.args[0] for call in cursor.execute.call_args_list]
        assert 'BEGIN "SH"."Proc$Load"; END;' in execute_calls


class TestCompareTwoSqlOracle:
    """Unit tests for OracleSandbox.compare_two_sql."""

    def test_invalid_sql_returns_syntax_error(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )

        result = backend.compare_two_sql(
            sandbox_db="SBX_ABC123000000",
            sql_a='SELECT ( FROM "SH"."CHANNELS"',
            sql_b='SELECT "CHANNEL_ID" FROM "SH"."CHANNELS"',
            fixtures=[],
        )

        assert result["status"] == "error"
        assert result["errors"][0]["code"] == "SQL_SYNTAX_ERROR"


# ── _parse_qualified_name ────────────────────────────────────────────────────


class TestParseQualifiedName:
    def test_splits_schema_and_table(self) -> None:
        schema, table = _parse_qualified_name("MIGRATIONTEST.BRONZE_CURRENCY")
        assert schema == "MIGRATIONTEST"
        assert table == "BRONZE_CURRENCY"

    def test_splits_lowercase(self) -> None:
        schema, table = _parse_qualified_name("sh.channels")
        assert schema == "sh"
        assert table == "channels"

    def test_rejects_bare_name(self) -> None:
        with pytest.raises(ValueError, match="Expected schema-qualified name"):
            _parse_qualified_name("CHANNELS")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Expected schema-qualified name"):
            _parse_qualified_name("")

    def test_rejects_trailing_dot(self) -> None:
        with pytest.raises(ValueError, match="Expected schema-qualified name"):
            _parse_qualified_name("SH.")

    def test_rejects_leading_dot(self) -> None:
        with pytest.raises(ValueError, match="Expected schema-qualified name"):
            _parse_qualified_name(".CHANNELS")

    def test_rejects_three_part_name(self) -> None:
        with pytest.raises(ValueError, match="Expected schema-qualified name"):
            _parse_qualified_name("DB.SCHEMA.TABLE")


# ── _validate_oracle_qualified_name ─────────────────────────────────────────


class TestValidateOracleQualifiedName:
    def test_accepts_bare_identifier(self) -> None:
        _validate_oracle_qualified_name("CHANNELS")  # should not raise

    def test_accepts_qualified_name(self) -> None:
        _validate_oracle_qualified_name("MIGRATIONTEST.BRONZE_CURRENCY")  # should not raise

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_qualified_name("")

    def test_rejects_injection_in_schema(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_qualified_name("BAD; DROP TABLE.CHANNELS")

    def test_rejects_injection_in_table(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_qualified_name("SH.CHANNELS; DROP TABLE CHANNELS--")

    def test_rejects_quoted_identifier(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_qualified_name('"SH".CHANNELS')

    def test_rejects_three_part_name(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_qualified_name("DB.SH.CHANNELS")
