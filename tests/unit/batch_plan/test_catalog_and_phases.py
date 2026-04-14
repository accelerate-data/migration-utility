"""Tests for catalog enumeration, phase classification, blocking deps, node building, and plan output.

Tests import shared.batch_plan internal helper functions directly for fast,
fixture-based execution.  No Docker or live database required.
"""

from __future__ import annotations

import json
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
)


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
