"""Tests for topological batching and build_batch_plan.

Tests import shared.batch_plan functions directly for fast, fixture-based
execution.  No Docker or live database required.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared.batch_plan import (
    _topological_batches,
    build_batch_plan,
)

from .conftest import _make_empty_project, _make_project


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
