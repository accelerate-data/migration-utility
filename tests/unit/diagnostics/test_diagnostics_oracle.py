"""Tests for shared.diagnostics.oracle — Oracle dialect-specific diagnostic checks."""

from __future__ import annotations

from pathlib import Path


from shared.diagnostics.oracle import (
    check_long_truncation,
    check_package_member,
    check_pipelined_function,
)
from shared.diagnostics.common import check_missing_reference
from shared.loader_data import DdlEntry

from diagnostics_helpers import (
    diag_empty_refs as _empty_refs,
    diag_make_ctx as _make_ctx,
)


# ── LONG_TRUNCATION ─────────────────────────────────────────────────────────


class TestLongTruncation:

    def test_fires_when_flag_is_true(self, tmp_path: Path) -> None:
        """LONG_TRUNCATION diagnostic fires when catalog_data has long_truncation: True."""
        catalog_data = {"long_truncation": True, "references": _empty_refs()}
        ctx = _make_ctx(tmp_path, "hr.big_view", "view", catalog_data, dialect="oracle")
        result = check_long_truncation(ctx)
        assert result is not None
        assert result.code == "LONG_TRUNCATION"
        assert result.severity == "error"
        assert "32,767 bytes" in result.message

    def test_returns_none_when_flag_absent(self, tmp_path: Path) -> None:
        """LONG_TRUNCATION returns None when catalog_data has no truncation flag."""
        catalog_data = {"references": _empty_refs()}
        ctx = _make_ctx(tmp_path, "hr.normal_view", "view", catalog_data, dialect="oracle")
        result = check_long_truncation(ctx)
        assert result is None


# ── PACKAGE_MEMBER ──────────────────────────────────────────────────────────


class TestPackageMember:

    def test_fires_for_package_member_ref(self, tmp_path: Path) -> None:
        """PACKAGE_MEMBER diagnostic fires when a reference matches a known package member."""
        refs = _empty_refs()
        refs["procedures"]["in_scope"] = [{"schema": "HR", "name": "PKG_PROC"}]
        catalog_data = {"references": refs}
        ctx = _make_ctx(
            tmp_path, "hr.my_view", "view", catalog_data,
            dialect="oracle",
            package_members={"hr.pkg_proc"},
        )
        result = check_package_member(ctx)
        assert result is not None
        assert len(result) == 1
        assert result[0].code == "PACKAGE_MEMBER"
        assert result[0].details["package_member_fqn"] == "hr.pkg_proc"

    def test_returns_none_when_ref_not_in_packages(self, tmp_path: Path) -> None:
        """PACKAGE_MEMBER returns None when refs do not match any package member."""
        refs = _empty_refs()
        refs["procedures"]["in_scope"] = [{"schema": "HR", "name": "STANDALONE_PROC"}]
        catalog_data = {"references": refs}
        ctx = _make_ctx(
            tmp_path, "hr.my_view", "view", catalog_data,
            dialect="oracle",
            package_members={"hr.pkg_proc"},
        )
        result = check_package_member(ctx)
        assert result is None

    def test_missing_reference_suppressed_for_package_member(self, tmp_path: Path) -> None:
        """MISSING_REFERENCE skips refs that are known package members."""
        refs = _empty_refs()
        refs["procedures"]["in_scope"] = [{"schema": "HR", "name": "PKG_PROC"}]
        catalog_data = {"references": refs}
        ctx = _make_ctx(
            tmp_path, "hr.my_view", "view", catalog_data,
            dialect="oracle",
            package_members={"hr.pkg_proc"},
            known_fqns={
                "tables": set(),
                "views": set(),
                "functions": set(),
                "procedures": set(),  # NOT in known — would normally fire MISSING_REFERENCE
                "materialized_views": set(),
            },
        )
        result = check_missing_reference(ctx)
        # MISSING_REFERENCE should NOT fire because the ref is a package member
        assert result is None


# ── PIPELINED_FUNCTION ──────────────────────────────────────────────────────


class TestPipelinedFunction:

    def test_fires_for_pipelined_function(self, tmp_path: Path) -> None:
        """PIPELINED_FUNCTION fires when DDL contains PIPELINED keyword."""
        ddl = """CREATE OR REPLACE FUNCTION hr.get_employees
RETURN emp_table_type PIPELINED IS
BEGIN
    FOR rec IN (SELECT * FROM employees) LOOP
        PIPE ROW(rec);
    END LOOP;
    RETURN;
END;"""
        entry = DdlEntry(raw_ddl=ddl, ast=None)
        catalog_data = {"references": _empty_refs()}
        ctx = _make_ctx(
            tmp_path, "hr.get_employees", "function", catalog_data,
            dialect="oracle", ddl_entry=entry,
        )
        result = check_pipelined_function(ctx)
        assert result is not None
        assert result.code == "PIPELINED_FUNCTION"
        assert result.severity == "warning"

    def test_returns_none_for_regular_function(self, tmp_path: Path) -> None:
        """PIPELINED_FUNCTION returns None for a regular function."""
        ddl = """CREATE OR REPLACE FUNCTION hr.calc_bonus(p_salary NUMBER)
RETURN NUMBER IS
BEGIN
    RETURN p_salary * 0.1;
END;"""
        entry = DdlEntry(raw_ddl=ddl, ast=None)
        catalog_data = {"references": _empty_refs()}
        ctx = _make_ctx(
            tmp_path, "hr.calc_bonus", "function", catalog_data,
            dialect="oracle", ddl_entry=entry,
        )
        result = check_pipelined_function(ctx)
        assert result is None

    def test_returns_none_when_no_ddl_entry(self, tmp_path: Path) -> None:
        """PIPELINED_FUNCTION returns None when ddl_entry is None."""
        catalog_data = {"references": _empty_refs()}
        ctx = _make_ctx(
            tmp_path, "hr.some_func", "function", catalog_data,
            dialect="oracle",
        )
        result = check_pipelined_function(ctx)
        assert result is None
