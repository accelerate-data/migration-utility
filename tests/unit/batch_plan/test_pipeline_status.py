"""Tests for pipeline status and dependency traversal.

Tests import shared.pipeline_status and shared.deps functions directly for fast,
fixture-based execution.  No Docker or live database required.
"""

from __future__ import annotations

import json

from shared.deps import collect_deps
from shared.pipeline_status import object_pipeline_status



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

    def test_table_n_a_seed(self, tmp_path):
        """Seed table returns n_a when pipeline status is queried directly."""
        dbt_root = tmp_path / "dbt"
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "catalog" / "tables" / "silver.lookup.json").write_text(
            json.dumps(
                {
                    "schema": "silver",
                    "name": "Lookup",
                    "is_seed": True,
                    "is_source": False,
                    "profile": {
                        "status": "ok",
                        "classification": {"resolved_kind": "seed", "source": "catalog"},
                    },
                },
            ),
            encoding="utf-8",
        )
        assert object_pipeline_status(tmp_path, "silver.lookup", "table", dbt_root) == "n_a"

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
