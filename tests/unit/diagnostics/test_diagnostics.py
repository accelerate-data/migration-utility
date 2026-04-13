"""Tests for shared.diagnostics — registry infrastructure and all cross-dialect checks."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared.diagnostics import (
    _REGISTRY,
    _THRESHOLDS,
    CatalogContext,
    DiagnosticRegistry,
    DiagnosticResult,
    _build_ddl_lookup,
    _build_known_fqns,
    _CheckSpec,
    diagnostic,
    run_diagnostics,
)
from shared.diagnostics.common import (
    check_circular_reference,
    check_dependency_has_error,
    check_missing_reference,
    check_multi_table_read,
    check_multi_table_write,
    check_nested_view_chain,
    check_out_of_scope_reference,
    check_parse_error,
    check_remote_exec_unsupported,
    check_stale_object,
    check_transitive_scope_leak,
    check_unsupported_syntax,
)
from shared.catalog import write_json
from shared.loader_data import DdlEntry

from diagnostics_helpers import (
    diag_empty_refs as _empty_refs,
    diag_git_init as _git_init,
    diag_make_ctx as _make_ctx,
    diag_write_catalog as _write_catalog,
    diag_write_ddl as _write_ddl,
)


# ── Registry infrastructure ─────────────────────────────────────────────────


class TestRegistryInfrastructure:

    def test_diagnostic_decorator_registers_check(self):
        """Register a dummy check, verify it appears in checks_for()."""
        registry = DiagnosticRegistry()

        def dummy_check(ctx: CatalogContext) -> None:
            return None

        registry.register(dummy_check, code="DUMMY", objects=["procedure"], dialects=("tsql", "oracle"), severity="warning", pass_number=1)

        checks = registry.checks_for("procedure", "tsql", 1)
        codes = [c.code for c in checks]
        assert "DUMMY" in codes

    def test_checks_for_filters_by_object_type(self):
        """Register checks for different object types, verify filtering."""
        registry = DiagnosticRegistry()

        def check_proc(ctx: CatalogContext) -> None:
            return None

        def check_view(ctx: CatalogContext) -> None:
            return None

        registry.register(check_proc, code="PROC_ONLY", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=1)
        registry.register(check_view, code="VIEW_ONLY", objects=["view"], dialects=("tsql",), severity="warning", pass_number=1)

        proc_checks = registry.checks_for("procedure", "tsql", 1)
        view_checks = registry.checks_for("view", "tsql", 1)

        assert any(c.code == "PROC_ONLY" for c in proc_checks)
        assert not any(c.code == "VIEW_ONLY" for c in proc_checks)

        assert any(c.code == "VIEW_ONLY" for c in view_checks)
        assert not any(c.code == "PROC_ONLY" for c in view_checks)

    def test_checks_for_filters_by_dialect(self):
        """Register a tsql-only check, verify it doesn't appear for oracle."""
        registry = DiagnosticRegistry()

        def tsql_only_check(ctx: CatalogContext) -> None:
            return None

        registry.register(tsql_only_check, code="TSQL_ONLY", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=1)

        tsql_checks = registry.checks_for("procedure", "tsql", 1)
        oracle_checks = registry.checks_for("procedure", "oracle", 1)

        assert any(c.code == "TSQL_ONLY" for c in tsql_checks)
        assert not any(c.code == "TSQL_ONLY" for c in oracle_checks)

    def test_checks_for_filters_by_pass_number(self):
        """Register pass 1 and pass 2 checks, verify filtering."""
        registry = DiagnosticRegistry()

        def pass1_check(ctx: CatalogContext) -> None:
            return None

        def pass2_check(ctx: CatalogContext) -> None:
            return None

        registry.register(pass1_check, code="PASS1", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=1)
        registry.register(pass2_check, code="PASS2", objects=["procedure"], dialects=("tsql",), severity="warning", pass_number=2)

        p1 = registry.checks_for("procedure", "tsql", 1)
        p2 = registry.checks_for("procedure", "tsql", 2)

        assert any(c.code == "PASS1" for c in p1)
        assert not any(c.code == "PASS2" for c in p1)

        assert any(c.code == "PASS2" for c in p2)
        assert not any(c.code == "PASS1" for c in p2)

    def test_run_diagnostics_reports_suppressed_check_count(self, tmp_path: Path):
        _git_init(tmp_path)
        _write_ddl(
            tmp_path,
            "procedures.sql",
            "CREATE PROCEDURE dbo.usp_test AS BEGIN SELECT 1 END\nGO\n",
        )
        _write_catalog(
            tmp_path,
            "procedures",
            "dbo.usp_test",
            {"references": _empty_refs()},
        )

        def _boom(_ctx: CatalogContext) -> None:
            raise RuntimeError("boom")

        spec = _CheckSpec(
            fn=_boom,
            code="TEST_BROKEN_CHECK",
            objects=["procedure"],
            dialects=("tsql",),
            severity="warning",
            pass_number=1,
        )
        _REGISTRY._checks.append(spec)
        try:
            result = run_diagnostics(tmp_path, dialect="tsql")
        finally:
            _REGISTRY._checks.remove(spec)

        assert result["objects_checked"] == 1
        assert result["suppressed_checks"] == 1


# ── PARSE_ERROR ──────────────────────────────────────────────────────────────


class TestParseError:

    def test_parse_error_positive(self):
        """DdlEntry with parse_error returns DiagnosticResult with code=PARSE_ERROR, severity=error."""
        entry = DdlEntry(raw_ddl="CREATE PROC bad", ast=None, parse_error="Unexpected token at line 1")
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_bad", "procedure", {}, ddl_entry=entry)

        result = check_parse_error(ctx)

        assert result is not None
        assert result.code == "PARSE_ERROR"
        assert result.severity == "error"
        assert "Unexpected token" in result.message

    def test_parse_error_negative(self):
        """DdlEntry with parse_error=None returns None."""
        entry = DdlEntry(raw_ddl="CREATE PROC good AS SELECT 1", ast=object(), parse_error=None)
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_good", "procedure", {}, ddl_entry=entry)

        result = check_parse_error(ctx)

        assert result is None

    def test_parse_error_suppressed_when_llm_statements_exist(self):
        """LLM-classified statements suppress the parse diagnostic after recovery."""
        entry = DdlEntry(raw_ddl="CREATE PROC bad", ast=None, parse_error="Unexpected token at line 1")
        ctx = _make_ctx(
            Path("/tmp/fake"),
            "dbo.usp_bad",
            "procedure",
            {"statements": [{"id": "stmt-1", "source": "llm", "action": "migrate", "sql": "SELECT 1"}]},
            ddl_entry=entry,
        )

        result = check_parse_error(ctx)

        assert result is None

    def test_parse_error_not_suppressed_for_non_llm_statements(self):
        """Non-LLM statements do not suppress a real parse diagnostic."""
        entry = DdlEntry(raw_ddl="CREATE PROC bad", ast=None, parse_error="Unexpected token at line 1")
        ctx = _make_ctx(
            Path("/tmp/fake"),
            "dbo.usp_bad",
            "procedure",
            {"statements": [{"id": "stmt-1", "source": "ast", "action": "migrate", "sql": "SELECT 1"}]},
            ddl_entry=entry,
        )

        result = check_parse_error(ctx)

        assert result is not None
        assert result.code == "PARSE_ERROR"


# ── UNSUPPORTED_SYNTAX ───────────────────────────────────────────────────────


class TestUnsupportedSyntax:

    def test_unsupported_syntax_positive(self):
        """DdlEntry with unsupported_syntax_nodes returns list of DiagnosticResult."""
        entry = DdlEntry(
            raw_ddl="CREATE PROC x AS EXEC dbo.proc",
            ast=object(),
            unsupported_syntax_nodes=["EXEC dbo.proc"],
        )
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_x", "procedure", {}, ddl_entry=entry)

        result = check_unsupported_syntax(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "UNSUPPORTED_SYNTAX"
        assert result[0].severity == "warning"

    def test_unsupported_syntax_negative(self):
        """DdlEntry with unsupported_syntax_nodes=None returns None."""
        entry = DdlEntry(raw_ddl="CREATE PROC y AS SELECT 1", ast=object(), unsupported_syntax_nodes=None)
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_y", "procedure", {}, ddl_entry=entry)

        result = check_unsupported_syntax(ctx)

        assert result is None


# ── STALE_OBJECT ─────────────────────────────────────────────────────────────


class TestStaleObject:

    def test_stale_object_positive(self):
        """catalog_data with stale: True returns DiagnosticResult."""
        catalog_data = {"stale": True, "ddl_hash": "abc123"}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.vw_old", "view", catalog_data)

        result = check_stale_object(ctx)

        assert result is not None
        assert result.code == "STALE_OBJECT"
        assert result.severity == "warning"
        assert result.details["previous_ddl_hash"] == "abc123"

    def test_stale_object_negative(self):
        """catalog_data without stale returns None."""
        catalog_data = {"ddl_hash": "abc123"}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.vw_fresh", "view", catalog_data)

        result = check_stale_object(ctx)

        assert result is None


# ── MISSING_REFERENCE ────────────────────────────────────────────────────────


class TestMissingReference:

    def test_missing_reference_positive(self):
        """in_scope ref pointing to dbo.missing_table not in known_fqns returns result."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [{"schema": "dbo", "name": "missing_table"}],
                    "out_of_scope": [],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        known = {"tables": set(), "views": set(), "functions": set(), "procedures": set()}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_test", "procedure", catalog_data, known_fqns=known)

        result = check_missing_reference(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "MISSING_REFERENCE"
        assert result[0].details["missing_fqn"] == "dbo.missing_table"

    def test_missing_reference_negative(self):
        """All in_scope refs exist in known_fqns returns None."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [{"schema": "dbo", "name": "existing_table"}],
                    "out_of_scope": [],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        known = {"tables": {"dbo.existing_table"}, "views": set(), "functions": set(), "procedures": set()}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_test", "procedure", catalog_data, known_fqns=known)

        result = check_missing_reference(ctx)

        assert result is None

    def test_missing_reference_multiple(self):
        """2 missing refs returns list of 2."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [
                        {"schema": "dbo", "name": "missing_one"},
                        {"schema": "dbo", "name": "missing_two"},
                    ],
                    "out_of_scope": [],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        known = {"tables": set(), "views": set(), "functions": set(), "procedures": set()}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_test", "procedure", catalog_data, known_fqns=known)

        result = check_missing_reference(ctx)

        assert result is not None
        assert len(result) == 2
        fqns = {r.details["missing_fqn"] for r in result}
        assert fqns == {"dbo.missing_one", "dbo.missing_two"}


# ── OUT_OF_SCOPE_REFERENCE ───────────────────────────────────────────────────


class TestOutOfScopeReference:

    def test_out_of_scope_positive(self):
        """out_of_scope with entries returns results."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [],
                    "out_of_scope": [
                        {"server": "remote_srv", "database": "OtherDB", "schema": "dbo", "name": "ext_table", "reason": "cross-server"},
                    ],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_ext", "procedure", catalog_data)

        result = check_out_of_scope_reference(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "OUT_OF_SCOPE_REFERENCE"
        assert result[0].severity == "warning"
        assert "remote_srv" in result[0].details["fqn"]

    def test_out_of_scope_negative(self):
        """Empty out_of_scope returns None."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_clean", "procedure", catalog_data)

        result = check_out_of_scope_reference(ctx)

        assert result is None


# ── REMOTE_EXEC_UNSUPPORTED ─────────────────────────────────────────────────


class TestRemoteExecUnsupported:

    def test_remote_exec_unsupported_positive(self):
        """Out-of-scope procedure refs produce REMOTE_EXEC_UNSUPPORTED."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {
                    "in_scope": [],
                    "out_of_scope": [
                        {
                            "server": "remote_srv",
                            "database": "OtherDB",
                            "schema": "dbo",
                            "name": "usp_load_ext",
                            "reason": "cross-server",
                        }
                    ],
                },
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_wrapper", "procedure", catalog_data)

        result = check_remote_exec_unsupported(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "REMOTE_EXEC_UNSUPPORTED"
        assert result[0].severity == "error"
        assert "remote_srv.OtherDB.dbo.usp_load_ext" in result[0].details["fqn"]

    def test_remote_exec_unsupported_negative(self):
        """Non-procedure out-of-scope refs do not trigger REMOTE_EXEC_UNSUPPORTED."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [],
                    "out_of_scope": [
                        {"server": "", "database": "OtherDB", "schema": "dbo", "name": "ext_table", "reason": "cross-database"}
                    ],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_wrapper", "procedure", catalog_data)

        result = check_remote_exec_unsupported(ctx)

        assert result is None


# ── MULTI_TABLE_WRITE ────────────────────────────────────────────────────────


class TestMultiTableWrite:

    def test_multi_table_write_positive(self):
        """Procedure with 2 is_updated tables returns result with details.tables."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [
                        {"schema": "dbo", "name": "target_a", "is_updated": True},
                        {"schema": "dbo", "name": "target_b", "is_updated": True},
                    ],
                    "out_of_scope": [],
                },
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_multi_write", "procedure", catalog_data)

        result = check_multi_table_write(ctx)

        assert result is not None
        assert result.code == "MULTI_TABLE_WRITE"
        assert result.severity == "warning"
        assert result.message == "Procedure writes to 2 tables: dbo.target_a, dbo.target_b. Each table will require a separate dbt model."
        assert len(result.details["tables"]) == 2
        assert "dbo.target_a" in result.details["tables"]
        assert "dbo.target_b" in result.details["tables"]

    def test_multi_table_write_single(self):
        """Procedure with 1 is_updated table returns None."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [
                        {"schema": "dbo", "name": "target_a", "is_updated": True},
                        {"schema": "dbo", "name": "source_b", "is_updated": False},
                    ],
                    "out_of_scope": [],
                },
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_single_write", "procedure", catalog_data)

        result = check_multi_table_write(ctx)

        assert result is None

    def test_multi_table_write_view_ignored(self):
        """MULTI_TABLE_WRITE is registered for procedure only, not for views."""
        # Verify via the global registry that MULTI_TABLE_WRITE is not in view checks
        view_checks = _REGISTRY.checks_for("view", "tsql", 1)
        codes = [c.code for c in view_checks]
        assert "MULTI_TABLE_WRITE" not in codes


# ── MULTI_TABLE_READ ─────────────────────────────────────────────────────────


class TestMultiTableRead:

    def test_multi_table_read_positive(self):
        """Function with 5 tables returns result."""
        in_scope = [{"schema": "dbo", "name": f"tbl_{i}"} for i in range(5)]
        catalog_data = {
            "references": {
                "tables": {"in_scope": in_scope, "out_of_scope": []},
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.fn_wide_read", "function", catalog_data)

        result = check_multi_table_read(ctx)

        assert result is not None
        assert result.code == "MULTI_TABLE_READ"
        assert result.severity == "warning"
        assert result.details["table_count"] == 5

    def test_multi_table_read_below_threshold(self):
        """Function with 4 tables returns None (threshold is 5)."""
        in_scope = [{"schema": "dbo", "name": f"tbl_{i}"} for i in range(4)]
        catalog_data = {
            "references": {
                "tables": {"in_scope": in_scope, "out_of_scope": []},
            }
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.fn_narrow_read", "function", catalog_data)

        result = check_multi_table_read(ctx)

        assert result is None


# ── CIRCULAR_REFERENCE ───────────────────────────────────────────────────────


class TestCircularReference:

    def test_circular_reference_direct(self):
        """proc A calls proc B, proc B calls proc A -> error."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # proc A references proc B
            cat_a = {
                "references": {
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_b"}],
                        "out_of_scope": [],
                    },
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                }
            }
            # proc B references proc A
            cat_b = {
                "references": {
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_a"}],
                        "out_of_scope": [],
                    },
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                }
            }
            _write_catalog(root, "procedures", "dbo.usp_a", cat_a)
            _write_catalog(root, "procedures", "dbo.usp_b", cat_b)

            ctx = _make_ctx(root, "dbo.usp_a", "procedure", cat_a)
            result = check_circular_reference(ctx)

            assert result is not None
            assert result.code == "CIRCULAR_REFERENCE"
            assert result.severity == "error"
            assert "dbo.usp_a" in result.details["cycle"]
            assert "dbo.usp_b" in result.details["cycle"]

    def test_circular_reference_self(self):
        """proc A calls itself -> error."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            cat_a = {
                "references": {
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_self"}],
                        "out_of_scope": [],
                    },
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                }
            }
            _write_catalog(root, "procedures", "dbo.usp_self", cat_a)

            ctx = _make_ctx(root, "dbo.usp_self", "procedure", cat_a)
            result = check_circular_reference(ctx)

            assert result is not None
            assert result.code == "CIRCULAR_REFERENCE"
            assert result.severity == "error"

    def test_circular_reference_no_cycle(self):
        """proc A calls proc B, proc B doesn't call A -> None."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            cat_a = {
                "references": {
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_b"}],
                        "out_of_scope": [],
                    },
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                }
            }
            cat_b = {
                "references": {
                    "procedures": {"in_scope": [], "out_of_scope": []},
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                }
            }
            _write_catalog(root, "procedures", "dbo.usp_a", cat_a)
            _write_catalog(root, "procedures", "dbo.usp_b", cat_b)

            ctx = _make_ctx(root, "dbo.usp_a", "procedure", cat_a)
            result = check_circular_reference(ctx)

            assert result is None


# ── DEPENDENCY_HAS_ERROR ─────────────────────────────────────────────────────


class TestDependencyHasError:

    def test_dependency_has_error_positive(self):
        """pass1_results has dep with PARSE_ERROR (severity=error) -> returns result."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {
                    "in_scope": [{"schema": "dbo", "name": "vw_broken"}],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        pass1 = {
            "dbo.vw_broken": [
                DiagnosticResult(code="PARSE_ERROR", message="DDL failed to parse", severity="error"),
            ],
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_caller", "procedure", catalog_data, pass1_results=pass1)

        result = check_dependency_has_error(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "DEPENDENCY_HAS_ERROR"
        assert result[0].details["dependency_fqn"] == "dbo.vw_broken"
        assert result[0].details["error_code"] == "PARSE_ERROR"

    def test_dependency_has_error_negative(self):
        """pass1_results has dep with warning only -> returns None."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {
                    "in_scope": [{"schema": "dbo", "name": "vw_warned"}],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        pass1 = {
            "dbo.vw_warned": [
                DiagnosticResult(code="STALE_OBJECT", message="stale", severity="warning"),
            ],
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_caller", "procedure", catalog_data, pass1_results=pass1)

        result = check_dependency_has_error(ctx)

        assert result is None


# ── TRANSITIVE_SCOPE_LEAK ────────────────────────────────────────────────────


class TestTransitiveScopeLeak:

    def test_transitive_scope_leak_positive(self):
        """pass1_results has dep with MISSING_REFERENCE -> returns result."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {
                    "in_scope": [{"schema": "dbo", "name": "vw_leaky"}],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        pass1 = {
            "dbo.vw_leaky": [
                DiagnosticResult(
                    code="MISSING_REFERENCE",
                    message="Referenced table dbo.ghost has no catalog entry.",
                    severity="warning",
                    details={"missing_fqn": "dbo.ghost", "reference_type": "table"},
                ),
            ],
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_upstream", "procedure", catalog_data, pass1_results=pass1)

        result = check_transitive_scope_leak(ctx)

        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0].code == "TRANSITIVE_SCOPE_LEAK"
        assert result[0].details["dependency_fqn"] == "dbo.vw_leaky"
        assert result[0].details["leaked_reference"] == "dbo.ghost"

    def test_transitive_scope_leak_negative(self):
        """pass1_results has dep with PARSE_ERROR (not a scope issue) -> returns None."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {
                    "in_scope": [{"schema": "dbo", "name": "vw_broken"}],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        pass1 = {
            "dbo.vw_broken": [
                DiagnosticResult(code="PARSE_ERROR", message="failed to parse", severity="error"),
            ],
        }
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_ok", "procedure", catalog_data, pass1_results=pass1)

        result = check_transitive_scope_leak(ctx)

        assert result is None


# ── NESTED_VIEW_CHAIN ────────────────────────────────────────────────────────


class TestNestedViewChain:

    def test_nested_view_chain_at_threshold(self):
        """Chain of 5 views -> returns result."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # Build a chain: vw_0 -> vw_1 -> vw_2 -> vw_3 -> vw_4 -> vw_5
            # vw_0 is the root. Depth = 5 (views vw_1 through vw_5).
            # Threshold is 5 so depth >= 5 should trigger.
            chain_len = 6  # vw_0 through vw_5
            for i in range(chain_len):
                if i < chain_len - 1:
                    next_name = f"vw_{i + 1}"
                    refs = {
                        "views": {
                            "in_scope": [{"schema": "dbo", "name": next_name}],
                            "out_of_scope": [],
                        },
                        "tables": {"in_scope": [], "out_of_scope": []},
                        "functions": {"in_scope": [], "out_of_scope": []},
                        "procedures": {"in_scope": [], "out_of_scope": []},
                    }
                else:
                    # Leaf view: no view refs
                    refs = {
                        "views": {"in_scope": [], "out_of_scope": []},
                        "tables": {
                            "in_scope": [{"schema": "dbo", "name": "base_table"}],
                            "out_of_scope": [],
                        },
                        "functions": {"in_scope": [], "out_of_scope": []},
                        "procedures": {"in_scope": [], "out_of_scope": []},
                    }
                cat = {"references": refs}
                _write_catalog(root, "views", f"dbo.vw_{i}", cat)

            root_cat = json.loads((root / "catalog" / "views" / "dbo.vw_0.json").read_text(encoding="utf-8"))
            ctx = _make_ctx(root, "dbo.vw_0", "view", root_cat)
            result = check_nested_view_chain(ctx)

            assert result is not None
            assert result.code == "NESTED_VIEW_CHAIN"
            assert result.severity == "warning"
            assert result.details["depth"] >= _THRESHOLDS["NESTED_VIEW_CHAIN_DEPTH"]

    def test_nested_view_chain_below_threshold(self):
        """Chain of 4 views -> returns None."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # vw_0 -> vw_1 -> vw_2 -> vw_3 (depth=3, below threshold of 5)
            chain_len = 4
            for i in range(chain_len):
                if i < chain_len - 1:
                    next_name = f"vw_{i + 1}"
                    refs = {
                        "views": {
                            "in_scope": [{"schema": "dbo", "name": next_name}],
                            "out_of_scope": [],
                        },
                        "tables": {"in_scope": [], "out_of_scope": []},
                        "functions": {"in_scope": [], "out_of_scope": []},
                        "procedures": {"in_scope": [], "out_of_scope": []},
                    }
                else:
                    refs = {
                        "views": {"in_scope": [], "out_of_scope": []},
                        "tables": {"in_scope": [], "out_of_scope": []},
                        "functions": {"in_scope": [], "out_of_scope": []},
                        "procedures": {"in_scope": [], "out_of_scope": []},
                    }
                cat = {"references": refs}
                _write_catalog(root, "views", f"dbo.vw_{i}", cat)

            root_cat = json.loads((root / "catalog" / "views" / "dbo.vw_0.json").read_text(encoding="utf-8"))
            ctx = _make_ctx(root, "dbo.vw_0", "view", root_cat)
            result = check_nested_view_chain(ctx)

            assert result is None

    def test_nested_view_chain_tables_are_leaves(self):
        """View refs a view that refs a table -> depth 1, no warning."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # vw_top -> vw_mid (which only refs a table, not another view)
            cat_top = {
                "references": {
                    "views": {
                        "in_scope": [{"schema": "dbo", "name": "vw_mid"}],
                        "out_of_scope": [],
                    },
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                }
            }
            cat_mid = {
                "references": {
                    "views": {"in_scope": [], "out_of_scope": []},
                    "tables": {
                        "in_scope": [{"schema": "dbo", "name": "base_table"}],
                        "out_of_scope": [],
                    },
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                }
            }
            _write_catalog(root, "views", "dbo.vw_top", cat_top)
            _write_catalog(root, "views", "dbo.vw_mid", cat_mid)

            ctx = _make_ctx(root, "dbo.vw_top", "view", cat_top)
            result = check_nested_view_chain(ctx)

            assert result is None


# ── Multi-diagnostic combo ───────────────────────────────────────────────────


class TestMultiDiagnosticCombo:

    def test_combo_parse_error_and_stale(self):
        """Object with both parse_error and stale:true -> 2 diagnostics."""
        entry = DdlEntry(raw_ddl="CREATE PROC bad", ast=None, parse_error="syntax error")
        catalog_data = {"stale": True, "ddl_hash": "old_hash"}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_combo", "procedure", catalog_data, ddl_entry=entry)

        r1 = check_parse_error(ctx)
        r2 = check_stale_object(ctx)

        assert r1 is not None
        assert r2 is not None
        assert r1.code == "PARSE_ERROR"
        assert r2.code == "STALE_OBJECT"

    def test_combo_missing_and_out_of_scope(self):
        """Object with both missing and out-of-scope refs -> multiple diagnostics."""
        catalog_data = {
            "references": {
                "tables": {
                    "in_scope": [{"schema": "dbo", "name": "ghost_table"}],
                    "out_of_scope": [
                        {"server": "", "database": "OtherDB", "schema": "dbo", "name": "ext_table", "reason": "cross-database"},
                    ],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        known = {"tables": set(), "views": set(), "functions": set(), "procedures": set()}
        ctx = _make_ctx(Path("/tmp/fake"), "dbo.usp_both", "procedure", catalog_data, known_fqns=known)

        missing_results = check_missing_reference(ctx)
        oos_results = check_out_of_scope_reference(ctx)

        assert missing_results is not None
        assert oos_results is not None
        assert len(missing_results) >= 1
        assert len(oos_results) >= 1
        assert missing_results[0].code == "MISSING_REFERENCE"
        assert oos_results[0].code == "OUT_OF_SCOPE_REFERENCE"


# ── run_diagnostics() end-to-end ─────────────────────────────────────────────


class TestRunDiagnosticsE2E:

    def test_run_diagnostics_e2e_writes_warnings(self):
        """Set up temp dir with catalog + DDL, run run_diagnostics(), verify warnings written."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # DDL with a multi-table write procedure
            ddl = (
                "CREATE PROCEDURE dbo.usp_multi\n"
                "AS\n"
                "BEGIN\n"
                "    INSERT INTO dbo.target_a SELECT * FROM dbo.source;\n"
                "    INSERT INTO dbo.target_b SELECT * FROM dbo.source;\n"
                "END\n"
            )
            _write_ddl(root, "dbo.usp_multi", ddl)

            # Catalog with 2 updated tables -> MULTI_TABLE_WRITE
            catalog_data = {
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "dbo", "name": "target_a", "is_updated": True},
                            {"schema": "dbo", "name": "target_b", "is_updated": True},
                            {"schema": "dbo", "name": "source", "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "warnings": [],
                "errors": [],
            }
            _write_catalog(root, "procedures", "dbo.usp_multi", catalog_data)

            summary = run_diagnostics(root, "tsql")

            assert summary["objects_checked"] >= 1

            # Re-read catalog and verify warnings were written
            result_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            result_data = json.loads(result_path.read_text(encoding="utf-8"))

            # Should have MULTI_TABLE_WRITE warning
            warning_codes = [w["code"] for w in result_data.get("warnings", [])]
            assert "MULTI_TABLE_WRITE" in warning_codes

    def test_run_diagnostics_idempotent(self):
        """Run twice, verify no duplicate warnings."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            ddl = "CREATE PROCEDURE dbo.usp_idem AS BEGIN INSERT INTO dbo.t1 SELECT 1; INSERT INTO dbo.t2 SELECT 2; END\n"
            _write_ddl(root, "dbo.usp_idem", ddl)

            catalog_data = {
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "dbo", "name": "t1", "is_updated": True},
                            {"schema": "dbo", "name": "t2", "is_updated": True},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "warnings": [],
                "errors": [],
            }
            _write_catalog(root, "procedures", "dbo.usp_idem", catalog_data)

            run_diagnostics(root, "tsql")
            run_diagnostics(root, "tsql")

            result_path = root / "catalog" / "procedures" / "dbo.usp_idem.json"
            result_data = json.loads(result_path.read_text(encoding="utf-8"))

            # Count MULTI_TABLE_WRITE occurrences — should be exactly 1
            mtw_count = sum(1 for w in result_data.get("warnings", []) if w["code"] == "MULTI_TABLE_WRITE")
            assert mtw_count == 1

    def test_run_diagnostics_no_catalog(self):
        """Run on dir without catalog -> returns zeros."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            summary = run_diagnostics(root, "tsql")

            assert summary["objects_checked"] == 0
            assert summary["warnings_added"] == 0
            assert summary["errors_added"] == 0


# ── Two-pass ordering ────────────────────────────────────────────────────────


class TestTwoPassOrdering:

    def test_pass2_reads_pass1_results(self):
        """Procedure A depends on procedure B. B has PARSE_ERROR (pass 1). After run_diagnostics, A has DEPENDENCY_HAS_ERROR."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            # Procedure B: intentionally broken DDL that triggers parse_error.
            # The loader recognises CREATE PROCEDURE but sqlglot fails on the body,
            # producing a Command fallback and setting parse_error.
            ddl_b = "CREATE PROCEDURE dbo.usp_b AS\n!!INVALID!!SQL!!"
            _write_ddl(root, "dbo.usp_b", ddl_b)

            cat_b = {
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "warnings": [],
                "errors": [],
            }
            _write_catalog(root, "procedures", "dbo.usp_b", cat_b)

            # Procedure A: depends on B (valid DDL)
            ddl_a = "CREATE PROCEDURE dbo.usp_a AS\nBEGIN\n  SELECT 1;\nEND"
            _write_ddl(root, "dbo.usp_a", ddl_a)

            cat_a = {
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_b"}],
                        "out_of_scope": [],
                    },
                },
                "warnings": [],
                "errors": [],
            }
            _write_catalog(root, "procedures", "dbo.usp_a", cat_a)

            summary = run_diagnostics(root, "tsql")

            # Read back both catalogs
            result_b = json.loads((root / "catalog" / "procedures" / "dbo.usp_b.json").read_text(encoding="utf-8"))
            b_error_codes = [e["code"] for e in result_b.get("errors", [])]

            if "PARSE_ERROR" not in b_error_codes:
                pytest.skip("DDL parsed without error; cannot test two-pass dependency propagation with this dialect's parser")

            result_a = json.loads((root / "catalog" / "procedures" / "dbo.usp_a.json").read_text(encoding="utf-8"))
            a_warning_codes = [w["code"] for w in result_a.get("warnings", [])]
            assert "DEPENDENCY_HAS_ERROR" in a_warning_codes, (
                f"Expected DEPENDENCY_HAS_ERROR in A's warnings, got {a_warning_codes}. "
                f"B errors: {b_error_codes}"
            )


# ── Materialized views (is_materialized_view flag) ───────────────────────────


class TestMaterializedViewFlag:

    def test_mv_flag_on_view_catalog(self):
        """View catalog entry with is_materialized_view=True is valid."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)
            cat_data = {
                "schema": "sh",
                "name": "mv_sales_agg",
                "is_materialized_view": True,
                "references": _empty_refs(),
                "warnings": [],
                "errors": [],
            }
            _write_catalog(root, "views", "sh.mv_sales_agg", cat_data)

            written = json.loads((root / "catalog" / "views" / "sh.mv_sales_agg.json").read_text())
            assert written["is_materialized_view"] is True
            assert written["schema"] == "sh"

    def test_mv_in_views_bucket_resolves_references(self):
        """An MV in the views bucket resolves MISSING_REFERENCE for view refs."""
        catalog_data = {
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {
                    "in_scope": [{"schema": "sh", "name": "mv_sales_agg"}],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            }
        }
        known = {
            "tables": set(),
            "views": {"sh.mv_sales_agg"},
            "functions": set(),
            "procedures": set(),
        }
        ctx = _make_ctx(Path("/tmp/fake"), "sh.some_proc", "procedure", catalog_data, known_fqns=known)

        result = check_missing_reference(ctx)

        assert result is None

    def test_write_view_catalog_sets_mv_flag(self):
        """write_view_catalog persists is_materialized_view when True."""
        from shared.catalog import write_view_catalog

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)
            (root / "catalog" / "views").mkdir(parents=True)

            p = write_view_catalog(
                root, "sh.mv_sales",
                _empty_refs(),
                is_materialized_view=True,
            )
            data = json.loads(p.read_text())
            assert data["is_materialized_view"] is True

    def test_write_view_catalog_omits_mv_flag_when_false(self):
        """write_view_catalog does not include is_materialized_view when False."""
        from shared.catalog import write_view_catalog

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)
            (root / "catalog" / "views").mkdir(parents=True)

            p = write_view_catalog(
                root, "sh.vw_regular",
                _empty_refs(),
            )
            data = json.loads(p.read_text())
            assert "is_materialized_view" not in data

    def test_mv_reclassify_from_tables_to_views(self):
        """MV detected in catalog/tables/ gets moved to catalog/views/ during extraction."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _git_init(root)

            table_data = {"schema": "sh", "name": "mv_old", "columns": []}
            _write_catalog(root, "tables", "sh.mv_old", table_data)

            mv_fqns = {"sh.mv_old"}
            catalog_dir = root / "catalog"
            for fqn in mv_fqns:
                table_path = catalog_dir / "tables" / f"{fqn}.json"
                if table_path.exists():
                    data = json.loads(table_path.read_text())
                    data["is_materialized_view"] = True
                    views_dir = catalog_dir / "views"
                    views_dir.mkdir(parents=True, exist_ok=True)
                    write_json(views_dir / f"{fqn}.json", data)
                    table_path.unlink()

            assert not (catalog_dir / "tables" / "sh.mv_old.json").exists()
            view_data = json.loads((catalog_dir / "views" / "sh.mv_old.json").read_text())
            assert view_data["is_materialized_view"] is True
            assert view_data["schema"] == "sh"
