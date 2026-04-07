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
    _topological_batches,
    build_batch_plan,
    collect_deps,
    collect_object_diagnostics,
    object_pipeline_status,
)

_TESTS_DIR = Path(__file__).parent
_FIXTURES = _TESTS_DIR / "fixtures" / "batch_plan"


# ── Project helpers ───────────────────────────────────────────────────────────


def _make_project(
    src: Path = _FIXTURES,
) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy fixtures to a temp dir and git-init it (required by resolve_project_root)."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "project"
    shutil.copytree(src, dst)
    subprocess.run(["git", "init"], cwd=dst, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=dst,
        capture_output=True,
        check=True,
        env={
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@t",
            "HOME": str(Path.home()),
        },
    )
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
    subprocess.run(["git", "init"], cwd=dst, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=dst,
        capture_output=True,
        check=True,
        env={
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@t",
            "HOME": str(Path.home()),
        },
    )
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
        """Table with test-spec but writer not refactored → refactor_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        spec_dir = tmp_path / "test-specs"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        spec_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "partial"}}),
            encoding="utf-8",
        )
        (spec_dir / "silver.t.json").write_text("{}", encoding="utf-8")
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "refactor_needed"

    def test_table_migrate_needed(self, tmp_path):
        """Fully prepared table with no dbt model → migrate_needed."""
        dbt_root = tmp_path / "dbt"
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        spec_dir = tmp_path / "test-specs"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        spec_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"}}),
            encoding="utf-8",
        )
        (spec_dir / "silver.t.json").write_text("{}", encoding="utf-8")
        assert object_pipeline_status(tmp_path, "silver.t", "table", dbt_root) == "migrate_needed"

    def test_table_complete(self, tmp_path):
        """Fully migrated table → complete."""
        dbt_root = tmp_path / "dbt"
        (dbt_root / "models" / "staging").mkdir(parents=True)
        (dbt_root / "models" / "staging" / "stg_t.sql").write_text("select 1", encoding="utf-8")
        cat_dir = tmp_path / "catalog" / "tables"
        proc_dir = tmp_path / "catalog" / "procedures"
        spec_dir = tmp_path / "test-specs"
        cat_dir.mkdir(parents=True)
        proc_dir.mkdir(parents=True)
        spec_dir.mkdir(parents=True)
        (cat_dir / "silver.t.json").write_text(
            json.dumps({
                "schema": "silver", "name": "T",
                "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_t"},
                "profile": {"status": "ok"},
            }),
            encoding="utf-8",
        )
        (proc_dir / "dbo.usp_load_t.json").write_text(
            json.dumps({"schema": "dbo", "name": "usp_load_t", "statements": [{"action": "migrate", "source": "ast", "sql": ""}], "mode": "deterministic", "routing_reasons": [], "refactor": {"status": "ok", "extracted_sql": "x", "refactored_sql": "y"}}),
            encoding="utf-8",
        )
        (spec_dir / "silver.t.json").write_text("{}", encoding="utf-8")
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

    def test_view_migrate_needed(self, tmp_path):
        """Analyzed view with no dbt model → migrate_needed."""
        dbt_root = tmp_path / "dbt"
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.vw_test", "view", dbt_root) == "migrate_needed"

    def test_view_complete(self, tmp_path):
        """Analyzed view with a dbt model → complete."""
        dbt_root = tmp_path / "dbt"
        (dbt_root / "models" / "staging").mkdir(parents=True)
        (dbt_root / "models" / "staging" / "stg_vw_test.sql").write_text("select 1", encoding="utf-8")
        view_dir = tmp_path / "catalog" / "views"
        view_dir.mkdir(parents=True)
        (view_dir / "silver.vw_test.json").write_text(
            json.dumps({"schema": "silver", "name": "vw_Test", "scoping": {"status": "analyzed"}, "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}, "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}}}),
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
            assert result["summary"]["total_objects"] == 0
            assert result["scope_phase"] == []
            assert result["profile_phase"] == []
            assert result["migrate_batches"] == []
        finally:
            tmp.cleanup()

    def test_schema_valid(self, assert_valid_schema):
        """build_batch_plan output conforms to batch_plan_output.json schema."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert_valid_schema(result, "batch_plan_output.json")
        finally:
            tmp.cleanup()

    def test_full_fixture_object_counts(self):
        """Fixture project produces correct counts per phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            summary = result["summary"]
            # 6 tables + 2 views
            assert summary["total_objects"] == 8
            assert summary["tables"] == 6
            assert summary["views"] == 2
            assert summary["writerless_tables"] == 1  # refcurrency
        finally:
            tmp.cleanup()

    def test_scope_phase_contents(self):
        """Objects needing scope land in scope_phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            scope_fqns = {n["fqn"] for n in result["scope_phase"]}
            assert "silver.dimdate" in scope_fqns      # no scoping
            assert "silver.vw_territory" in scope_fqns  # view not analyzed
        finally:
            tmp.cleanup()

    def test_profile_phase_contents(self):
        """Scoped-but-unprofiled objects land in profile_phase."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            profile_fqns = {n["fqn"] for n in result["profile_phase"]}
            assert "silver.dimgeography" in profile_fqns
        finally:
            tmp.cleanup()

    def test_n_a_objects(self):
        """Writerless tables appear in n_a_objects."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            na_fqns = {o["fqn"] for o in result["n_a_objects"]}
            assert "silver.refcurrency" in na_fqns
        finally:
            tmp.cleanup()

    def test_completed_objects(self):
        """Fully migrated objects appear in completed_objects."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            done_fqns = {n["fqn"] for n in result["completed_objects"]}
            assert "silver.dimcustomer" in done_fqns
        finally:
            tmp.cleanup()

    def test_migrate_batches_exist(self):
        """Migrate-phase objects are split into at least two batches."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            assert len(result["migrate_batches"]) >= 2
        finally:
            tmp.cleanup()

    def test_factsales_in_later_batch(self):
        """factsales depends on dimproduct → lands in a batch after dimproduct."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            batch_for = {}
            for batch in result["migrate_batches"]:
                for node in batch["objects"]:
                    batch_for[node["fqn"]] = batch["batch"]

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
                for batch in result["migrate_batches"]
                for n in batch["objects"]
                if n["fqn"] == "silver.factsales"
            )
            blocking = set(factsales_node["blocking_deps"])
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

    def test_node_has_dbt_model_flag(self):
        """completed objects have has_dbt_model=True; others have False."""
        tmp, dst = _make_project()
        try:
            result = build_batch_plan(dst)
            for node in result["completed_objects"]:
                assert node["has_dbt_model"] is True, node["fqn"]
            for node in result["scope_phase"]:
                assert node["has_dbt_model"] is False, node["fqn"]
        finally:
            tmp.cleanup()


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
            warning_codes = [w["code"] for w in result["catalog_diagnostics"]["warnings"]]
            assert "STALE_OBJECT" in warning_codes
            assert result["catalog_diagnostics"]["total_warnings"] >= 1
        finally:
            tmp.cleanup()
