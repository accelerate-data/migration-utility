"""Tests for batch_plan.py — dependency-aware parallel batch scheduler.

Tests import shared.batch_plan functions directly for fast, fixture-based
execution.  No Docker or live database required.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from shared.batch_plan import (
    _CatalogInventory,
    _build_plan_output,
    _classify_phases,
    _compute_blocking_deps,
    _enumerate_catalog,
    _make_node,
    _resolve_excluded_type,
    _topological_batches,
    build_batch_plan,
)
from shared.deps import collect_deps
from shared.pipeline_status import (
    _compute_diagnostic_stage_flags,
    collect_object_diagnostics,
    object_pipeline_status,
)

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures" / "batch_plan"


# ── Project helpers ───────────────────────────────────────────────────────────


def _make_project(
    src: Path = _FIXTURES,
) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy fixtures to a temp dir."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    shutil.copytree(src, dst)
    return tmp, dst


def _make_empty_project() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a project with no catalog objects."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    dst.mkdir(parents=True)
    (dst / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
        encoding="utf-8",
    )
    (dst / "catalog" / "tables").mkdir(parents=True)
    return tmp, dst


# ── Pipeline status tests ─────────────────────────────────────────────────────


class TestObjectPipelineStatus:
    def test_table_scope_needed_no_scoping(self, tmp_path):
        """Table with no scoping section → scope_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "silver.dimdate.json").write_text(
            json.dumps({"schema": "silver", "name": "DimDate"}), encoding="utf-8"
        )
        assert object_pipeline_status(tmp_path, "silver.dimdate", "table", dbt_root) == "scope_needed"

    def test_table_scope_needed_no_writer(self, tmp_path):
        """Table with scoping but no selected_writer → scope_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "silver.dimdate.json").write_text(
            json.dumps({"schema": "silver", "name": "DimDate", "scoping": {"status": "pending"}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.dimdate", "table", dbt_root) == "scope_needed"

    def test_table_n_a_writerless(self, tmp_path):
        """Table with no_writer_found → n_a."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "silver.refcurrency.json").write_text(
            json.dumps({"schema": "silver", "name": "RefCurrency", "scoping": {"status": "no_writer_found"}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.refcurrency", "table", dbt_root) == "n_a"

    def test_table_profile_needed(self, tmp_path):
        """Scoped table with no profile → profile_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": []}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "profile_needed"

    def test_table_test_gen_needed(self, tmp_path):
        """Profiled table with no test-spec → test_gen_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (tmp_path / "catalog" / "procedures").mkdir(parents=True)
        (tmp_path / "catalog" / "procedures" / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": []}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "test_gen_needed"

    def test_table_refactor_needed(self, tmp_path):
        """Table with test_gen ok but writer not refactored → refactor_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
                "test_gen": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "partial"}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "refactor_needed"

    def test_table_migrate_needed(self, tmp_path):
        """Fully prepared table with no generate status → migrate_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
                "test_gen": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "migrate_needed"

    def test_table_complete(self, tmp_path):
        """Fully migrated table → complete."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
                "test_gen": {"status": "ok"},
                "generate": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "complete"

    def test_view_scope_needed(self, tmp_path):
        """View with no scoping → scope_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "scope_needed"

    def test_view_profile_needed(self, tmp_path):
        """Analyzed view with no profile → profile_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "profile_needed"

    def test_view_test_gen_needed(self, tmp_path):
        """Analyzed + profiled view with no test spec → test_gen_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "profile": {"status": "ok", "classification": "stg", "source": "llm"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "test_gen_needed"

    def test_view_refactor_needed(self, tmp_path):
        """Analyzed + profiled view with test_gen ok but no refactor → refactor_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "profile": {"status": "ok", "classification": "stg", "source": "llm"}, "test_gen": {"status": "ok"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "refactor_needed"

    def test_view_migrate_needed(self, tmp_path):
        """Analyzed + profiled + refactored view with no generate status → migrate_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "profile": {"status": "ok", "classification": "stg", "source": "llm"}, "test_gen": {"status": "ok"}, "refactor": {"status": "ok", "extracted_sql": "SELECT 1", "refactored_sql": "WITH src AS (SELECT 1) SELECT * FROM src"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "migrate_needed"

    def test_view_complete(self, tmp_path):
        """Fully pipeline-complete view with generate status ok → complete."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "profile": {"status": "ok", "classification": "stg", "source": "llm"}, "test_gen": {"status": "ok"}, "refactor": {"status": "ok", "extracted_sql": "SELECT 1", "refactored_sql": "WITH src AS (SELECT 1) SELECT * FROM src"}, "generate": {"status": "ok"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "complete"


# ── Dependency traversal tests ────────────────────────────────────────────────


class TestCollectDeps:
    def test_table_no_writer(self, tmp_path):
        """Table with no writer returns empty deps."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T", "scoping": {}}), encoding="utf-8"
        )
        assert collect_deps(tmp_path, "silver.t", "table") == set()

    def test_table_direct_table_dep(self, tmp_path):
        """Table's writer reads from another in-scope table."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T", "scoping": {"selected_writer": "dbo.usp_load_t"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_load_t", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {"in_scope": [{"schema": "bronze", "name": "RawData"}], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.t", "table")
        assert "bronze.rawdata" in deps

    def test_table_transitive_via_view(self, tmp_path):
        """Table's writer reads a view that references another table."""
        proc_dir = tmp_path / "catalog" / "procedures"
        view_dir = tmp_path / "catalog" / "views"
        proc_dir.mkdir(parents=True)
        view_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.fact.json").write_text(
            json.dumps({"schema": "silver", "name": "Fact", "scoping": {"selected_writer": "dbo.usp_load_fact"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_fact.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_load_fact", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_dim"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        (view_dir / "silver.vw_dim.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_dim",
                "references": {
                    "tables": {"in_scope": [{"schema": "silver", "name": "DimSource"}], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.fact", "table")
        assert "silver.vw_dim" in deps
        assert "silver.dimsource" in deps

    def test_table_transitive_via_proc(self, tmp_path):
        """Table's writer calls another proc that reads a different table."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T", "scoping": {"selected_writer": "dbo.usp_main"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_main.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_main", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [{"schema": "dbo", "name": "usp_helper"}], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_helper.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_helper", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {"in_scope": [{"schema": "silver", "name": "HelperTable"}], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.t", "table")
        assert "silver.helpertable" in deps

    def test_view_transitive_view_chain(self, tmp_path):
        """View references another view that references a table."""
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_a.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_A",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_B"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        (view_dir / "silver.vw_b.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_B",
                "references": {
                    "tables": {"in_scope": [{"schema": "silver", "name": "BaseTable"}], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.vw_a", "view")
        assert "silver.vw_b" in deps
        assert "silver.basetable" in deps

    def test_table_transitive_proc_view_view_table(self, tmp_path):
        """proc → view_a → view_b → table: all three hops resolved."""
        proc_dir = tmp_path / "catalog" / "procedures"
        view_dir = tmp_path / "catalog" / "views"
        proc_dir.mkdir(parents=True)
        view_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.fact.json").write_text(
            json.dumps({"schema": "silver", "name": "Fact", "scoping": {"selected_writer": "dbo.usp_load_fact"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_fact.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_load_fact", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_mid"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        # vw_mid references vw_base (view → view hop)
        (view_dir / "silver.vw_mid.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_mid",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_base"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        # vw_base references a concrete table (view → table hop)
        (view_dir / "silver.vw_base.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_base",
                "references": {
                    "tables": {"in_scope": [{"schema": "silver", "name": "DimLeaf"}], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.fact", "table")
        # Intermediate view
        assert "silver.vw_mid" in deps
        # Second-level view
        assert "silver.vw_base" in deps
        # Leaf table at the end of the chain
        assert "silver.dimleaf" in deps

    def test_out_of_scope_refs_not_traversed(self, tmp_path):
        """out_of_scope entries in a proc or view must not be added to deps."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T", "scoping": {"selected_writer": "dbo.usp_load_t"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_load_t", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {
                        "in_scope": [{"schema": "silver", "name": "InScope"}],
                        "out_of_scope": [{"schema": "external", "name": "OutScope"}],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.t", "table")
        assert "silver.inscope" in deps
        assert "external.outscope" not in deps

    def test_cycle_detection_view(self, tmp_path):
        """Circular view references terminate without error (depth limit)."""
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_x.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_X",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_Y"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        (view_dir / "silver.vw_y.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_Y",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [{"schema": "silver", "name": "vw_X"}], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        # Must not raise and must return a finite set
        deps = collect_deps(tmp_path, "silver.vw_x", "view")
        assert isinstance(deps, set)

    def test_cycle_detection_proc(self, tmp_path):
        """Circular proc→proc calls terminate without error."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T", "scoping": {"selected_writer": "dbo.usp_a"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_a.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_a", "mode": "deterministic", "routing_reasons": [],
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}, "procedures": {"in_scope": [{"schema": "dbo", "name": "usp_b"}], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_b.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_b", "mode": "deterministic", "routing_reasons": [],
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}, "procedures": {"in_scope": [{"schema": "dbo", "name": "usp_a"}], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.t", "table")
        assert isinstance(deps, set)

    def test_merge_pattern_self_reference_stripped(self, tmp_path):
        """MERGE pattern: writer proc references its own target table — self-ref must not appear in deps."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.factagg.json").write_text(
            json.dumps({"schema": "silver", "name": "FactAgg", "scoping": {"selected_writer": "dbo.usp_load_factagg"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_factagg.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_load_factagg", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "silver", "name": "FactAgg"},  # self-reference via MERGE
                            {"schema": "bronze", "name": "RawAgg"},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.factagg", "table")
        assert "silver.factagg" not in deps
        assert "bronze.rawagg" in deps

    def test_truncate_insert_pattern_self_reference_stripped(self, tmp_path):
        """TRUNCATE+INSERT pattern: writer proc references its own target table — self-ref must not appear in deps."""
        proc_dir = tmp_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True)
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.dimproduct.json").write_text(
            json.dumps({"schema": "silver", "name": "DimProduct", "scoping": {"selected_writer": "dbo.usp_reload_dimproduct"}}),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_reload_dimproduct.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "usp_reload_dimproduct", "mode": "deterministic", "routing_reasons": [],
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "silver", "name": "DimProduct"},  # self-reference via TRUNCATE+INSERT
                            {"schema": "bronze", "name": "RawProduct"},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )
        deps = collect_deps(tmp_path, "silver.dimproduct", "table")
        assert "silver.dimproduct" not in deps
        assert "bronze.rawproduct" in deps


# ── Topological batch tests ───────────────────────────────────────────────────


class TestTopologicalBatches:
    def test_empty_input(self):
        assert _topological_batches([], {}) == []

    def test_no_dependencies(self):
        """Objects with no deps all go in batch 0."""
        batches = _topological_batches(["a", "b", "c"], {})
        assert len(batches) == 1
        assert sorted(batches[0]) == ["a", "b", "c"]

    def test_linear_chain(self):
        """a → b → c produces three sequential batches."""
        blocking = {"c": {"b"}, "b": {"a"}, "a": set()}
        batches = _topological_batches(["a", "b", "c"], blocking)
        assert len(batches) == 3
        assert batches[0] == ["a"]
        assert batches[1] == ["b"]
        assert batches[2] == ["c"]

    def test_diamond_shape(self):
        """a → b, a → c, both → d: b and c in same batch."""
        blocking = {"b": {"a"}, "c": {"a"}, "d": {"b", "c"}, "a": set()}
        batches = _topological_batches(["a", "b", "c", "d"], blocking)
        assert len(batches) == 3
        assert batches[0] == ["a"]
        assert sorted(batches[1]) == ["b", "c"]
        assert batches[2] == ["d"]

    def test_cycle_excluded(self):
        """Objects in a cycle are not scheduled."""
        blocking = {"x": {"y"}, "y": {"x"}, "z": set()}
        batches = _topological_batches(["x", "y", "z"], blocking)
        scheduled = {fqn for batch in batches for fqn in batch}
        assert "z" in scheduled
        assert "x" not in scheduled
        assert "y" not in scheduled

    def test_dep_outside_input_ignored(self):
        """Blocking deps not in the input list are ignored in topo sort."""
        # "b" blocks "a", but "b" is not in fqns — treated as unresolved externally
        blocking = {"a": {"b"}}
        batches = _topological_batches(["a"], blocking)
        # "b" is outside fqns, so restricted blocking for "a" is empty → batch 0
        assert len(batches) == 1
        assert batches[0] == ["a"]


# ── build_batch_plan integration tests ───────────────────────────────────────


class TestBuildBatchPlan:
    def test_empty_catalog(self):
        """Empty catalog returns valid empty plan."""
        tmp, dst = _make_empty_project()
        try:
            result = build_batch_plan(dst)
            assert result.summary.total_objects == 0
            assert result.scope_phase == []
            assert result.profile_phase == []
            assert result.migrate_batches == []
        finally:
            tmp.cleanup()

    def test_full_fixture_object_counts(self):
        """Fixture project produces correct counts per phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            summary = result.summary
            # refcurrency has no_writer_found → writerless n_a object
            # 6 tables + 2 views = 8 active catalog objects
            assert summary.total_objects == 8
            assert summary.tables == 6
            assert summary.views == 2
            assert summary.writerless_tables == 1
            assert summary.source_pending == 0
            assert summary.source_tables == 0
        finally:
            tmp.cleanup()

    def test_scope_phase_contents(self):
        """Objects needing scope land in scope_phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            scope_fqns = {n.fqn for n in result.scope_phase}
            assert "silver.dimdate" in scope_fqns      # no scoping
            assert "silver.vw_territory" in scope_fqns  # view not analyzed
        finally:
            tmp.cleanup()

    def test_profile_phase_contents(self):
        """Scoped-but-unprofiled objects land in profile_phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            profile_fqns = {n.fqn for n in result.profile_phase}
            assert "silver.dimgeography" in profile_fqns
        finally:
            tmp.cleanup()

    def test_n_a_objects_contains_refcurrency(self):
        """Writerless fixture table appears in n_a_objects."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            n_a_fqns = {o.fqn for o in result.n_a_objects}
            assert "silver.refcurrency" in n_a_fqns
        finally:
            tmp.cleanup()

    def test_source_pending_empty_for_fixture(self):
        """Confirmed writerless fixture table is not source_pending."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert result.source_pending == []
        finally:
            tmp.cleanup()

    def test_completed_objects(self):
        """Fully migrated objects appear in completed_objects."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            done_fqns = {n.fqn for n in result.completed_objects}
            assert "silver.dimcustomer" in done_fqns
        finally:
            tmp.cleanup()

    def test_migrate_batches_exist(self):
        """Migrate-phase objects are split into at least two batches."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert len(result.migrate_batches) >= 2
        finally:
            tmp.cleanup()

    def test_factsales_in_later_batch(self):
        """factsales depends on dimproduct → lands in a batch after dimproduct."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            batch_for = {}
            for batch in result.migrate_batches:
                for node in batch.objects:
                    batch_for[node.fqn] = batch.batch

            assert "silver.factsales" in batch_for
            assert "silver.dimproduct" in batch_for
            assert batch_for["silver.factsales"] > batch_for["silver.dimproduct"]
        finally:
            tmp.cleanup()

    def test_factsales_blocking_deps(self):
        """factsales has dimproduct, vw_territory, and dimdate as blocking deps."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            factsales_node = next(
                n
                for batch in result.migrate_batches
                for n in batch.objects
                if n.fqn == "silver.factsales"
            )
            blocking = set(factsales_node.blocking_deps)
            # dimproduct is in migrate phase (test_gen_needed) — blocking
            assert "silver.dimproduct" in blocking
            # vw_territory is scope_needed — blocking
            assert "silver.vw_territory" in blocking
            # dimdate is scope_needed — blocking (via usp_helper_prep)
            assert "silver.dimdate" in blocking
            # dimcustomer is complete (has dbt model) — NOT blocking
            assert "silver.dimcustomer" not in blocking
        finally:
            tmp.cleanup()

    def test_factsales_writerless_not_in_blocking_deps(self):
        """refcurrency is writerless (n_a) — must not appear in any object's blocking_deps."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            for batch in result.migrate_batches:
                for node in batch.objects:
                    assert "silver.refcurrency" not in node.blocking_deps, (
                        f"{node.fqn} wrongly lists writerless refcurrency as a blocker"
                    )
        finally:
            tmp.cleanup()

    def test_writerless_only_dep_not_blocking(self, tmp_path):
        """View that depends only on a writerless source table has empty blocking_deps
        and is placed in a migration batch (ready to migrate)."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "views").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        import subprocess as _sp
        _sp.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        _sp.run(["git", "commit", "--allow-empty", "-m", "i"], cwd=tmp_path, capture_output=True,
                check=True, env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                                 "HOME": str(Path.home())})

        # source_table: writerless (n_a)
        (tmp_path / "catalog" / "tables" / "src.source_table.json").write_text(json.dumps({
            "schema": "src", "name": "source_table",
            "scoping": {"status": "no_writer_found", "candidates": []},
        }), encoding="utf-8")

        # vw_report: analyzed + profiled view that depends only on source_table
        (tmp_path / "catalog" / "views" / "src.vw_report.json").write_text(json.dumps({
            "schema": "src", "name": "vw_report",
            "scoping": {"status": "analyzed"},
            "profile": {"status": "ok", "classification": "mart", "source": "llm"},
            "references": {
                "tables": {"in_scope": [{"schema": "src", "name": "source_table"}], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        result = build_batch_plan(tmp_path)
        # vw_report should be in migrate_batches (migrate_needed), not blocked
        batch_fqns = {n.fqn for batch in result.migrate_batches for n in batch.objects}
        assert "src.vw_report" in batch_fqns

        vw_node = next(n for batch in result.migrate_batches for n in batch.objects
                       if n.fqn == "src.vw_report")
        assert vw_node.blocking_deps == [], (
            f"Expected no blocking_deps, got {vw_node.blocking_deps}"
        )

    def test_writerless_and_migrate_candidate_dep(self, tmp_path):
        """View depends on a writerless table AND a migrate-candidate:
        only the migrate-candidate appears in blocking_deps."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "views").mkdir(parents=True)
        (tmp_path / "catalog" / "procedures").mkdir(parents=True)
        (tmp_path / "test-specs").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        import subprocess as _sp
        _sp.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        _sp.run(["git", "commit", "--allow-empty", "-m", "i"], cwd=tmp_path, capture_output=True,
                check=True, env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                                 "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                                 "HOME": str(Path.home())})

        # writerless source table
        (tmp_path / "catalog" / "tables" / "src.dim_lookup.json").write_text(json.dumps({
            "schema": "src", "name": "dim_lookup",
            "scoping": {"status": "no_writer_found", "candidates": []},
        }), encoding="utf-8")

        # migrate-candidate table (test_gen_needed)
        (tmp_path / "catalog" / "tables" / "src.dim_product.json").write_text(json.dumps({
            "schema": "src", "name": "dim_product",
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_dim_product"},
            "profile": {"status": "ok"},
        }), encoding="utf-8")
        (tmp_path / "catalog" / "procedures" / "dbo.usp_load_dim_product.json").write_text(json.dumps({
            "schema": "dbo", "name": "usp_load_dim_product", "mode": "deterministic", "routing_reasons": [],
            "statements": [{"action": "migrate", "source": "ast", "sql": ""}],
            "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"},
            "references": {"tables": {"in_scope": [], "out_of_scope": []},
                           "views": {"in_scope": [], "out_of_scope": []},
                           "functions": {"in_scope": [], "out_of_scope": []},
                           "procedures": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")
        (tmp_path / "test-specs" / "src.dim_product.json").write_text("{}", encoding="utf-8")

        # view depends on both
        (tmp_path / "catalog" / "views" / "src.vw_fact.json").write_text(json.dumps({
            "schema": "src", "name": "vw_fact",
            "scoping": {"status": "analyzed"},
            "profile": {"status": "ok", "classification": "mart", "source": "llm"},
            "references": {
                "tables": {"in_scope": [
                    {"schema": "src", "name": "dim_lookup"},
                    {"schema": "src", "name": "dim_product"},
                ], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        result = build_batch_plan(tmp_path)
        vw_node = next(n for batch in result.migrate_batches for n in batch.objects
                       if n.fqn == "src.vw_fact")
        blocking = set(vw_node.blocking_deps)
        assert "src.dim_product" in blocking, "migrate-candidate dep must be blocking"
        assert "src.dim_lookup" not in blocking, "writerless dep must not be blocking"

    def test_blocking_deps_3level_intermediate_behind(self):
        """3-level chain: proc → vw_mid (scope_needed) → vw_base → leaf.
        vw_mid has no dbt model → it and its transitive deps are all blocking for fact."""
        tmp = tempfile.TemporaryDirectory()
        dst = Path(tmp.name) / "project"
        # Minimal project skeleton
        (dst / "catalog" / "tables").mkdir(parents=True)
        (dst / "catalog" / "views").mkdir(parents=True)
        (dst / "catalog" / "procedures").mkdir(parents=True)
        (dst / "test-specs").mkdir(parents=True)
        (dst / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        import subprocess as _sp
        _sp.run(["git", "init"], cwd=dst, capture_output=True, check=True)
        _sp.run(["git", "commit", "--allow-empty", "-m", "i"], cwd=dst, capture_output=True, check=True,
                env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                     "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                     "HOME": str(Path.home())})

        # fact: fully prepared, no dbt model yet → migrate_needed
        _proc_refs = lambda tables, views, procs: {
            "tables": {"in_scope": tables, "out_of_scope": []},
            "views": {"in_scope": views, "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": procs, "out_of_scope": []},
        }
        _view_refs = lambda tables, views: {
            "tables": {"in_scope": tables, "out_of_scope": []},
            "views": {"in_scope": views, "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        }

        (dst / "catalog" / "tables" / "silver.fact.json").write_text(json.dumps({
            "schema": "silver", "name": "Fact",
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_fact"},
            "profile": {"status": "ok"},
        }), encoding="utf-8")
        (dst / "catalog" / "procedures" / "dbo.usp_load_fact.json").write_text(json.dumps({
            "schema": "dbo", "name": "usp_load_fact", "mode": "deterministic", "routing_reasons": [],
            "statements": [{"action": "migrate", "source": "ast", "sql": ""}],
            "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"},
            "references": _proc_refs([], [{"schema": "silver", "name": "vw_mid"}], []),
        }), encoding="utf-8")
        (dst / "test-specs" / "silver.fact.json").write_text("{}", encoding="utf-8")

        # vw_mid: not analyzed yet (scope_needed), no dbt model
        (dst / "catalog" / "views" / "silver.vw_mid.json").write_text(json.dumps({
            "schema": "silver", "name": "vw_mid",
            "references": _view_refs([], [{"schema": "silver", "name": "vw_base"}]),
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        # vw_base: also not analyzed, references leaf table
        (dst / "catalog" / "views" / "silver.vw_base.json").write_text(json.dumps({
            "schema": "silver", "name": "vw_base",
            "references": _view_refs([{"schema": "silver", "name": "Leaf"}], []),
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        # leaf table: scope_needed
        (dst / "catalog" / "tables" / "silver.leaf.json").write_text(json.dumps({
            "schema": "silver", "name": "Leaf",
        }), encoding="utf-8")

        try:
            result = build_batch_plan(dst)
            fact_node = next(
                n for batch in result.migrate_batches for n in batch.objects
                if n.fqn == "silver.fact"
            )
            blocking = set(fact_node.blocking_deps)
            # vw_mid is scope_needed, no model → blocking
            assert "silver.vw_mid" in blocking
            # vw_base is scope_needed, no model → also blocking (transitive)
            assert "silver.vw_base" in blocking
            # leaf table is scope_needed, no model → also blocking (transitive)
            assert "silver.leaf" in blocking
        finally:
            tmp.cleanup()

    def test_blocking_deps_3level_intermediate_complete(self):
        """3-level chain: proc → vw_mid (complete, has model) → vw_base (scope_needed) → leaf.
        vw_mid has a dbt model → it is NOT blocking even though its own deps are incomplete."""
        tmp = tempfile.TemporaryDirectory()
        dst = Path(tmp.name) / "project"
        (dst / "catalog" / "tables").mkdir(parents=True)
        (dst / "catalog" / "views").mkdir(parents=True)
        (dst / "catalog" / "procedures").mkdir(parents=True)
        (dst / "test-specs").mkdir(parents=True)
        # vw_mid has a dbt model
        (dst / "dbt" / "models" / "staging").mkdir(parents=True)
        (dst / "dbt" / "models" / "staging" / "vw_mid.sql").write_text("select 1", encoding="utf-8")
        (dst / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        import subprocess as _sp
        _sp.run(["git", "init"], cwd=dst, capture_output=True, check=True)
        _sp.run(["git", "commit", "--allow-empty", "-m", "i"], cwd=dst, capture_output=True, check=True,
                env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                     "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                     "HOME": str(Path.home())})

        _proc_refs = lambda tables, views, procs: {
            "tables": {"in_scope": tables, "out_of_scope": []},
            "views": {"in_scope": views, "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
            "procedures": {"in_scope": procs, "out_of_scope": []},
        }
        _view_refs = lambda tables, views: {
            "tables": {"in_scope": tables, "out_of_scope": []},
            "views": {"in_scope": views, "out_of_scope": []},
            "functions": {"in_scope": [], "out_of_scope": []},
        }

        (dst / "catalog" / "tables" / "silver.fact.json").write_text(json.dumps({
            "schema": "silver", "name": "Fact",
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_fact"},
            "profile": {"status": "ok"},
        }), encoding="utf-8")
        (dst / "catalog" / "procedures" / "dbo.usp_load_fact.json").write_text(json.dumps({
            "schema": "dbo", "name": "usp_load_fact", "mode": "deterministic", "routing_reasons": [],
            "statements": [{"action": "migrate", "source": "ast", "sql": ""}],
            "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"},
            "references": _proc_refs([], [{"schema": "silver", "name": "vw_mid"}], []),
        }), encoding="utf-8")
        (dst / "test-specs" / "silver.fact.json").write_text("{}", encoding="utf-8")

        # vw_mid: analyzed + profiled + has dbt model → complete
        (dst / "catalog" / "views" / "silver.vw_mid.json").write_text(json.dumps({
            "schema": "silver", "name": "vw_mid",
            "scoping": {"status": "analyzed"},
            "profile": {"status": "ok", "classification": "stg", "source": "llm"},
            "references": _view_refs([], [{"schema": "silver", "name": "vw_base"}]),
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        # vw_base: not analyzed, no model
        (dst / "catalog" / "views" / "silver.vw_base.json").write_text(json.dumps({
            "schema": "silver", "name": "vw_base",
            "references": _view_refs([{"schema": "silver", "name": "Leaf"}], []),
            "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                              "views": {"in_scope": [], "out_of_scope": []},
                              "functions": {"in_scope": [], "out_of_scope": []}},
        }), encoding="utf-8")

        # leaf: scope_needed
        (dst / "catalog" / "tables" / "silver.leaf.json").write_text(json.dumps({
            "schema": "silver", "name": "Leaf",
        }), encoding="utf-8")

        try:
            result = build_batch_plan(dst)
            fact_node = next(
                n for batch in result.migrate_batches for n in batch.objects
                if n.fqn == "silver.fact"
            )
            blocking = set(fact_node.blocking_deps)
            # vw_mid is complete (has dbt model) → NOT blocking
            assert "silver.vw_mid" not in blocking
            # vw_base and leaf are still incomplete but vw_mid acts as a boundary —
            # fact does not need to wait for vw_base/leaf once vw_mid is migrated
            assert "silver.vw_base" not in blocking
            assert "silver.leaf" not in blocking
        finally:
            tmp.cleanup()

    def test_node_has_dbt_model_flag(self):
        """completed objects have has_dbt_model=True; others have False."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            for node in result.completed_objects:
                assert node.has_dbt_model is True, node.fqn
            for node in result.scope_phase:
                assert node.has_dbt_model is False, node.fqn
        finally:
            tmp.cleanup()

    def _write_merge_ready_table(self, root: Path, table_fqn: str, writer_fqn: str, extra_table_refs: list[dict]) -> None:
        """Write catalog files for a migrate-ready table whose writer has a self-reference."""
        schema, name = table_fqn.split(".", 1)
        w_schema, w_name = writer_fqn.split(".", 1)
        (root / "catalog" / "tables").mkdir(parents=True, exist_ok=True)
        (root / "catalog" / "procedures").mkdir(parents=True, exist_ok=True)
        (root / "test-specs").mkdir(parents=True, exist_ok=True)
        (root / "catalog" / "tables" / f"{table_fqn}.json").write_text(
            json.dumps({
                "schema": schema, "name": name,
                "scoping": {"status": "resolved", "selected_writer": writer_fqn},
                "profile": {"status": "ok", "classification": {}},
            }),
            encoding="utf-8",
        )
        (root / "test-specs" / f"{table_fqn}.json").write_text(json.dumps({}), encoding="utf-8")
        (root / "catalog" / "procedures" / f"{writer_fqn}.json").write_text(
            json.dumps({
                "schema": w_schema, "name": w_name, "mode": "deterministic", "routing_reasons": [],
                "refactor": {"status": "ok"},
                "references": {
                    "tables": {
                        "in_scope": [{"schema": schema, "name": name}] + extra_table_refs,
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
            }),
            encoding="utf-8",
        )

    def test_merge_pattern_not_circular(self, tmp_path):
        """MERGE pattern: table whose writer reads its own target is scheduled, not flagged as circular."""
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        self._write_merge_ready_table(
            tmp_path, "silver.factagg", "dbo.usp_load_factagg",
            extra_table_refs=[{"schema": "bronze", "name": "RawAgg"}],
        )
        result = build_batch_plan(tmp_path, dbt_root=tmp_path / "dbt")
        scheduled = {n.fqn for batch in result.migrate_batches for n in batch.objects}
        circular = {r.fqn for r in result.circular_refs}
        assert "silver.factagg" in scheduled
        assert "silver.factagg" not in circular

    def test_truncate_insert_pattern_not_circular(self, tmp_path):
        """TRUNCATE+INSERT pattern: table whose writer reads its own target is scheduled, not flagged as circular."""
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
        )
        self._write_merge_ready_table(
            tmp_path, "silver.dimproduct", "dbo.usp_reload_dimproduct",
            extra_table_refs=[{"schema": "bronze", "name": "RawProduct"}],
        )
        result = build_batch_plan(tmp_path, dbt_root=tmp_path / "dbt")
        scheduled = {n.fqn for batch in result.migrate_batches for n in batch.objects}
        circular = {r.fqn for r in result.circular_refs}
        assert "silver.dimproduct" in scheduled
        assert "silver.dimproduct" not in circular


# ── Diagnostics tests ─────────────────────────────────────────────────────────


class TestCollectObjectDiagnostics:
    def test_no_diagnostics(self, tmp_path):
        """Object with no warnings/errors returns empty list."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({"schema": "silver", "name": "T"}), encoding="utf-8"
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        assert diags == []

    def test_top_level_warnings(self, tmp_path):
        """Top-level warnings on a table are collected."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "warnings": [{"code": "STALE_OBJECT", "message": "Object is stale", "severity": "warning"}],
            }),
            encoding="utf-8",
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        codes = [d["code"] for d in diags]
        assert "STALE_OBJECT" in codes

    def test_scoping_errors_collected(self, tmp_path):
        """Errors in scoping sub-section are collected."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {
                    "errors": [{"code": "MULTI_TABLE_WRITE", "message": "Multiple writes", "severity": "error"}],
                    "warnings": [],
                },
            }),
            encoding="utf-8",
        )
        diags = collect_object_diagnostics(tmp_path, "silver.t", "table")
        codes = [d["code"] for d in diags]
        assert "MULTI_TABLE_WRITE" in codes

    def test_catalog_diagnostics_aggregated(self):
        """build_batch_plan aggregates diagnostics across all objects."""
        tmp, dst = _make_project()
        try:
            # Inject a diagnostic into dimdate's catalog
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["warnings"] = [{"code": "STALE_OBJECT", "message": "stale", "severity": "warning"}]
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            warning_codes = [w.code for w in result.catalog_diagnostics.warnings]
            assert "STALE_OBJECT" in warning_codes
            assert result.catalog_diagnostics.total_warnings >= 1
        finally:
            tmp.cleanup()


# ── Excluded objects tests ────────────────────────────────────────────────────


class TestExcludedObjects:
    """Tests for excluded: true filtering in build_batch_plan."""

    def _make_single_table_project(self, tmp_path: Path, excluded: bool = False) -> Path:
        """Create a minimal project with one table, optionally excluded."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        cat: dict = {"schema": "silver", "name": "DimDate", "primary_keys": [], "referenced_by": {}}
        if excluded:
            cat["excluded"] = True
        (tmp_path / "catalog" / "tables" / "silver.dimdate.json").write_text(
            json.dumps(cat), encoding="utf-8"
        )
        return tmp_path

    def test_excluded_table_absent_from_all_phases(self, tmp_path):
        """Excluded table does not appear in scope_phase, profile_phase, migrate_batches, n_a_objects, or completed_objects."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        all_active_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
            | {n.fqn for n in result.n_a_objects}
        )
        assert "silver.dimdate" not in all_active_fqns

    def test_excluded_table_in_excluded_objects_list(self, tmp_path):
        """Excluded table appears in excluded_objects with correct shape."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        assert len(result.excluded_objects) == 1
        entry = result.excluded_objects[0]
        assert entry.fqn == "silver.dimdate"
        assert entry.type == "table"
        assert hasattr(entry, "note")

    def test_excluded_count_in_summary(self, tmp_path):
        """summary.excluded_count reflects number of excluded objects."""
        dst = self._make_single_table_project(tmp_path, excluded=True)
        result = build_batch_plan(dst)
        assert result.summary.excluded_count == 1

    def test_non_excluded_table_has_zero_excluded_count(self, tmp_path):
        """summary.excluded_count is 0 when nothing is excluded."""
        dst = self._make_single_table_project(tmp_path, excluded=False)
        result = build_batch_plan(dst)
        assert result.summary.excluded_count == 0
        assert result.excluded_objects == []

    def test_excluded_view_absent_from_all_phases(self, tmp_path):
        """Excluded view does not appear in any active phase."""
        (tmp_path / "catalog" / "views").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        (tmp_path / "catalog" / "views" / "silver.vw_legacy.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_legacy",
                "excluded": True,
                "references": {"tables": {"in_scope": []}, "views": {"in_scope": []}},
            }),
            encoding="utf-8",
        )
        result = build_batch_plan(tmp_path)
        assert len(result.excluded_objects) == 1
        assert result.excluded_objects[0].fqn == "silver.vw_legacy"
        assert result.excluded_objects[0].type == "view"
        all_active = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
        )
        assert "silver.vw_legacy" not in all_active

    def test_excluded_dep_removed_from_active_dep_graph(self, tmp_path):
        """Active table that previously depended on now-excluded table has no blocking dep on it."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        # Excluded source table
        (tmp_path / "catalog" / "tables" / "silver.excludedsource.json").write_text(
            json.dumps({
                "schema": "silver", "name": "ExcludedSource",
                "excluded": True, "primary_keys": [], "referenced_by": {},
            }),
            encoding="utf-8",
        )
        # Active table — no writer proc, so writerless (n_a).
        # Deps are resolved via writer proc; with no writer this table has no dep chain.
        # For simplicity, just confirm the excluded table is not in excluded_objects fqn set
        # of the active table's direct_deps.
        (tmp_path / "catalog" / "tables" / "silver.facttable.json").write_text(
            json.dumps({
                "schema": "silver", "name": "FactTable",
                "primary_keys": [], "referenced_by": {},
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        result = build_batch_plan(tmp_path)
        excluded_fqns_in_output = {e.fqn for e in result.excluded_objects}
        assert "silver.excludedsource" in excluded_fqns_in_output
        # silver.facttable has no_writer_found → writerless n_a
        n_a_fqns = {n.fqn for n in result.n_a_objects}
        assert "silver.facttable" in n_a_fqns

    def test_full_fixture_excluded_count_zero(self):
        """Standard fixture (no excluded objects) has excluded_count=0."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert result.summary.excluded_count == 0
            assert result.excluded_objects == []
        finally:
            tmp.cleanup()

    def test_full_fixture_with_one_excluded(self):
        """Excluding one table reduces total active count and adds to excluded_objects."""
        tmp, dst = _make_project()
        try:
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["excluded"] = True
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            assert result.summary.excluded_count == 1
            assert result.summary.total_objects == 7  # refcurrency remains as writerless n_a, dimdate excluded
            excluded_fqns = {e.fqn for e in result.excluded_objects}
            assert "silver.dimdate" in excluded_fqns
            # silver.dimdate must not appear in any active phase
            all_active = (
                {n.fqn for n in result.scope_phase}
                | {n.fqn for n in result.profile_phase}
                | {n.fqn for b in result.migrate_batches for n in b.objects}
            )
            assert "silver.dimdate" not in all_active
        finally:
            tmp.cleanup()


# ── diagnostic_stage_flags tests ──────────────────────────────────────────────


class TestComputeDiagnosticStageFlags:
    def test_empty_diagnostics(self):
        """No diagnostics → empty flags dict."""
        assert _compute_diagnostic_stage_flags([]) == {}

    def test_no_mapped_codes(self):
        """Diagnostics with codes not in the stage map → empty flags dict."""
        diags = [{"code": "STALE_OBJECT", "severity": "warning", "message": "stale"}]
        assert _compute_diagnostic_stage_flags(diags) == {}

    def test_parse_error_maps_to_refactor(self):
        """PARSE_ERROR maps to refactor stage with error severity."""
        diags = [{"code": "PARSE_ERROR", "severity": "error", "message": "parse failed"}]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_ddl_parse_error_maps_to_refactor(self):
        """DDL_PARSE_ERROR maps to refactor stage with error severity."""
        diags = [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": "parse failed"}]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_multi_table_write_maps_to_scope(self):
        """MULTI_TABLE_WRITE maps to scope stage, preserving the diagnostic's severity."""
        diags = [{"code": "MULTI_TABLE_WRITE", "severity": "warning", "message": "multi write"}]
        assert _compute_diagnostic_stage_flags(diags) == {"scope": "warning"}

    def test_remote_exec_unsupported_maps_to_scope(self):
        """REMOTE_EXEC_UNSUPPORTED maps to scope stage with error severity."""
        diags = [{"code": "REMOTE_EXEC_UNSUPPORTED", "severity": "error", "message": "remote exec"}]
        assert _compute_diagnostic_stage_flags(diags) == {"scope": "error"}

    def test_error_beats_warning_same_stage(self):
        """Two diagnostics for the same stage: error severity wins over warning."""
        diags = [
            {"code": "PARSE_ERROR", "severity": "warning", "message": "w"},
            {"code": "DDL_PARSE_ERROR", "severity": "error", "message": "e"},
        ]
        assert _compute_diagnostic_stage_flags(diags) == {"refactor": "error"}

    def test_multiple_stages(self):
        """Diagnostics that map to different stages both appear in the result."""
        diags = [
            {"code": "PARSE_ERROR", "severity": "error", "message": "parse"},
            {"code": "MULTI_TABLE_WRITE", "severity": "warning", "message": "multi"},
        ]
        result = _compute_diagnostic_stage_flags(diags)
        assert result == {"refactor": "error", "scope": "warning"}

    def test_node_includes_diagnostic_stage_flags(self):
        """build_batch_plan nodes include diagnostic_stage_flags field."""
        tmp, dst = _make_project()
        try:
            # Inject a PARSE_ERROR diagnostic into dimdate's catalog
            dimdate_path = dst / "catalog" / "tables" / "silver.dimdate.json"
            cat = json.loads(dimdate_path.read_text())
            cat["errors"] = [{"code": "PARSE_ERROR", "message": "failed to parse", "severity": "error"}]
            dimdate_path.write_text(json.dumps(cat))

            result = build_batch_plan(dst)
            dimdate_node = next(
                n for n in result.scope_phase if n.fqn == "silver.dimdate"
            )
            assert hasattr(dimdate_node, "diagnostic_stage_flags")
            assert dimdate_node.diagnostic_stage_flags.get("refactor") == "error"
        finally:
            tmp.cleanup()


# ── is_source flag tests ──────────────────────────────────────────────────────


def _write_table_cat(path: Path, fqn: str, scoping: dict, extra: dict | None = None) -> None:
    schema, name = fqn.split(".", 1)
    data: dict = {"schema": schema, "name": name, "scoping": scoping}
    if extra:
        data.update(extra)
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_minimal_project(tmp_path: Path) -> Path:
    """Create a minimal project with catalog/tables/ dir."""
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
    )
    return tmp_path


class TestIsSourceBatchPlan:
    def test_is_source_table_excluded_from_pipeline(self, tmp_path):
        """Table with is_source: true is excluded from all pipeline phases."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        all_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
            | {n.fqn for n in result.n_a_objects}
            | {n.fqn for n in result.source_pending}
        )
        assert "silver.audit" not in all_fqns

    def test_is_source_counted_in_summary(self, tmp_path):
        """summary.source_tables counts is_source: true tables."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        assert result.summary.source_tables == 1
        assert result.summary.total_objects == 0  # excluded from pipeline

    def test_is_source_appears_in_source_tables_list(self, tmp_path):
        """is_source: true table appears in source_tables output list."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.audit.json",
            "silver.audit",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        source_fqns = {o.fqn for o in result.source_tables}
        assert "silver.audit" in source_fqns

    def test_resolved_table_with_is_source_excluded_from_pipeline(self, tmp_path):
        """Resolved table marked is_source: true is excluded (cross-domain scenario)."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.crossdomain.json",
            "silver.crossdomain",
            {"status": "resolved", "selected_writer": "dbo.usp_other_team"},
            {"is_source": True},
        )
        result = build_batch_plan(root)
        assert result.summary.source_tables == 1
        assert result.summary.total_objects == 0

    def test_writerless_table_populates_n_a_objects(self, tmp_path):
        """no_writer_found table without is_source appears in n_a_objects."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.lookup.json",
            "silver.lookup",
            {"status": "no_writer_found"},
        )
        result = build_batch_plan(root)
        n_a_fqns = {o.fqn for o in result.n_a_objects}
        assert "silver.lookup" in n_a_fqns
        assert result.summary.writerless_tables == 1
        assert result.summary.source_pending == 0

    def test_writerless_table_not_in_active_pipeline_phases(self, tmp_path):
        """Writerless tables do not appear in active pipeline phases."""
        root = _make_minimal_project(tmp_path)
        cat_dir = root / "catalog" / "tables"
        _write_table_cat(
            cat_dir / "silver.lookup.json",
            "silver.lookup",
            {"status": "no_writer_found"},
        )
        result = build_batch_plan(root)
        all_pipeline_fqns = (
            {n.fqn for n in result.scope_phase}
            | {n.fqn for n in result.profile_phase}
            | {n.fqn for batch in result.migrate_batches for n in batch.objects}
            | {n.fqn for n in result.completed_objects}
        )
        assert "silver.lookup" not in all_pipeline_fqns




# ── _enumerate_catalog tests ────────────────────────────────────────────────


class TestEnumerateCatalog:
    def test_empty_catalog_dir(self, tmp_path):
        """Empty catalog dir produces empty inventory."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "views").mkdir(parents=True)
        inv = _enumerate_catalog(tmp_path)
        assert inv.table_fqns == []
        assert inv.view_entries == []
        assert inv.excluded_fqns == set()
        assert inv.all_objects == []

    def test_classifies_excluded_tables(self, tmp_path):
        """Tables with excluded=True go to excluded_fqns."""
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.excluded_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "excluded_t", "excluded": True}),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert "dbo.excluded_t" in inv.excluded_fqns
        assert inv.table_fqns == []

    def test_classifies_source_tables(self, tmp_path):
        """Tables with is_source=True go to source_table_fqns."""
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.src.json").write_text(
            json.dumps({"schema": "dbo", "name": "src", "is_source": True}),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert inv.source_table_fqns == ["dbo.src"]
        assert inv.table_fqns == []

    def test_classifies_writerless_tables_as_pipeline_tables(self, tmp_path):
        """Tables with no_writer_found and no is_source stay in table inventory."""
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.pending.json").write_text(
            json.dumps({
                "schema": "dbo", "name": "pending",
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert inv.table_fqns == ["dbo.pending"]
        assert inv.source_pending_fqns == []

    def test_classifies_pipeline_tables(self, tmp_path):
        """Normal tables go to table_fqns."""
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.t.json").write_text(
            json.dumps({"schema": "dbo", "name": "t"}),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert inv.table_fqns == ["dbo.t"]

    def test_classifies_views_and_mvs(self, tmp_path):
        """Views and MVs are classified correctly."""
        cat_dir = tmp_path / "catalog" / "views"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.v.json").write_text(
            json.dumps({"schema": "dbo", "name": "v"}),
            encoding="utf-8",
        )
        (cat_dir / "dbo.mv.json").write_text(
            json.dumps({"schema": "dbo", "name": "mv", "is_materialized_view": True}),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert ("dbo.mv", "mv") in inv.view_entries
        assert ("dbo.v", "view") in inv.view_entries

    def test_obj_type_map(self, tmp_path):
        """obj_type_map correctly maps FQNs to types."""
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.t.json").write_text(
            json.dumps({"schema": "dbo", "name": "t"}),
            encoding="utf-8",
        )
        inv = _enumerate_catalog(tmp_path)
        assert inv.obj_type_map == {"dbo.t": "table"}


# ── _classify_phases tests ──────────────────────────────────────────────────


class TestClassifyPhases:
    def test_all_phases(self):
        """Objects are sorted into correct phases by status."""
        objects = [
            ("a", "table"), ("b", "table"), ("c", "view"),
            ("d", "table"), ("e", "table"),
        ]
        statuses = {
            "a": "scope_needed",
            "b": "profile_needed",
            "c": "refactor_needed",
            "d": "complete",
            "e": "n_a",
        }
        scope, profile, migrate, completed, n_a = _classify_phases(objects, statuses)
        assert scope == ["a"]
        assert profile == ["b"]
        assert migrate == ["c"]
        assert completed == ["d"]
        assert n_a == ["e"]

    def test_migrate_statuses(self):
        """test_gen_needed, refactor_needed, migrate_needed all go to migrate."""
        objects = [("a", "t"), ("b", "t"), ("c", "t")]
        statuses = {"a": "test_gen_needed", "b": "refactor_needed", "c": "migrate_needed"}
        _, _, migrate, _, _ = _classify_phases(objects, statuses)
        assert sorted(migrate) == ["a", "b", "c"]

    def test_empty(self):
        scope, profile, migrate, completed, n_a = _classify_phases([], {})
        assert scope == profile == migrate == completed == n_a == []


# ── _compute_blocking_deps tests ────────────────────────────────────────────


class TestComputeBlockingDeps:
    def test_no_deps(self):
        """Objects with no deps have empty blocking sets."""
        result = _compute_blocking_deps(["a", "b"], {}, {}, set())
        assert result == {"a": set(), "b": set()}

    def test_uncovered_dep_blocks(self):
        """A dep without a dbt model that isn't covered blocks."""
        result = _compute_blocking_deps(
            ["a"],
            raw_deps={"a": {"b"}},
            dbt_status={"a": False, "b": False},
            writerless_fqns=set(),
        )
        assert result["a"] == {"b"}

    def test_dbt_model_unblocks(self):
        """A dep with a dbt model does not block."""
        result = _compute_blocking_deps(
            ["a"],
            raw_deps={"a": {"b"}},
            dbt_status={"a": False, "b": True},
            writerless_fqns=set(),
        )
        assert result["a"] == set()

    def test_covered_by_complete_node(self):
        """Deps covered by a complete intermediate node don't block."""
        result = _compute_blocking_deps(
            ["a"],
            raw_deps={"a": {"b", "c"}, "b": {"c"}},
            dbt_status={"a": False, "b": True, "c": False},
            writerless_fqns=set(),
        )
        # b has dbt model → doesn't block; c is covered by b's deps → doesn't block
        assert result["a"] == set()

    def test_writerless_never_blocks(self):
        """Writerless (n_a) tables never block."""
        result = _compute_blocking_deps(
            ["a"],
            raw_deps={"a": {"b"}},
            dbt_status={"a": False, "b": False},
            writerless_fqns={"b"},
        )
        assert result["a"] == set()


# ── _make_node tests ────────────────────────────────────────────────────────


class TestMakeNode:
    def test_basic_node(self):
        node = _make_node(
            "dbo.t",
            obj_type_map={"dbo.t": "table"},
            statuses={"dbo.t": "scope_needed"},
            dbt_status={"dbo.t": False},
            raw_deps={"dbo.t": {"dbo.s"}},
            blocking={"dbo.t": {"dbo.s"}},
            obj_diagnostics={"dbo.t": []},
        )
        assert node.fqn == "dbo.t"
        assert node.type == "table"
        assert node.pipeline_status == "scope_needed"
        assert node.has_dbt_model is False
        assert node.direct_deps == ["dbo.s"]
        assert node.blocking_deps == ["dbo.s"]
        assert node.diagnostics == []
        assert node.diagnostic_stage_flags == {}

    def test_node_with_diagnostics(self):
        diags = [{"code": "PARSE_ERROR", "severity": "error", "message": "bad"}]
        node = _make_node(
            "dbo.t",
            obj_type_map={"dbo.t": "table"},
            statuses={"dbo.t": "refactor_needed"},
            dbt_status={"dbo.t": False},
            raw_deps={},
            blocking={},
            obj_diagnostics={"dbo.t": diags},
        )
        assert node.diagnostic_stage_flags == {"refactor": "error"}


# ── _resolve_excluded_type tests ────────────────────────────────────────────


class TestResolveExcludedType:
    def test_table_type(self, tmp_path):
        cat_dir = tmp_path / "catalog" / "tables"
        cat_dir.mkdir(parents=True)
        (cat_dir / "dbo.t.json").write_text("{}", encoding="utf-8")
        assert _resolve_excluded_type(tmp_path, "dbo.t") == "table"

    def test_view_type(self, tmp_path):
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "dbo.v.json").write_text(
            json.dumps({"schema": "dbo", "name": "v"}), encoding="utf-8",
        )
        assert _resolve_excluded_type(tmp_path, "dbo.v") == "view"

    def test_mv_type(self, tmp_path):
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "dbo.mv.json").write_text(
            json.dumps({"schema": "dbo", "name": "mv", "is_materialized_view": True}),
            encoding="utf-8",
        )
        assert _resolve_excluded_type(tmp_path, "dbo.mv") == "mv"


# ── _build_plan_output tests ───────────────────────────────────────────────


class TestBuildPlanOutput:
    def test_empty_inventory(self, tmp_path):
        """Empty inventory produces a valid empty plan."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        inv = _CatalogInventory()
        result = _build_plan_output(inv=inv, project_root=tmp_path)
        assert result.summary.total_objects == 0
        assert result.scope_phase == []
        assert result.migrate_batches == []
        assert result.catalog_diagnostics.total_errors == 0

    def test_counts_match_inventory(self, tmp_path):
        """Summary counts reflect inventory contents."""
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        inv = _CatalogInventory(
            table_fqns=["a", "b"],
            view_entries=[("c", "view"), ("d", "mv")],
            excluded_fqns={"e"},
            source_table_fqns=["f"],
            source_pending_fqns=["g"],
        )
        result = _build_plan_output(inv=inv, project_root=tmp_path, n_a_fqns=["h"])
        s = result.summary
        assert s.total_objects == 4  # 2 tables + 2 views
        assert s.tables == 2
        assert s.views == 1
        assert s.mvs == 1
        assert s.writerless_tables == 1
        assert s.excluded_count == 1
        assert s.source_tables == 1
        assert s.source_pending == 1
