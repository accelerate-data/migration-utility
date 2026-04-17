from __future__ import annotations

import json
import tempfile
from pathlib import Path


from shared import dry_run
from shared.output_models.dry_run import DryRunOutput
from tests.unit.dry_run.dry_run_test_helpers import (
    _cli_runner,
    _make_bare_project,
    _make_project,
)

def test_ready_scope_passes() -> None:
    """Object scope ready when manifest and catalog file exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is True
        assert result.project.reason == "ok"
        assert result.object.reason == "ok"

def test_ready_scope_no_manifest() -> None:
    """Scope not ready when manifest is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").unlink()
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.ready is False
        assert result.object is None
        assert result.project.reason == "manifest_missing"

def test_ready_scope_no_catalog_file() -> None:
    """Scope not ready when catalog file does not exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.NonExistent")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is False
        assert result.object.reason == "object_not_found"
        assert result.object.object_type is None
        assert result.object.code == "OBJECT_NOT_FOUND"

def test_ready_setup_ddl_passes_with_manifest() -> None:
    """setup-ddl project readiness uses the manifest gate with no object input."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "setup-ddl")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is None
        assert result.project.reason == "ok"

def test_ready_profile_passes() -> None:
    """Profile ready when scoping.status == resolved."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_profile_not_scoped() -> None:
    """Profile not ready when table has no scoping section."""
    tmp, root = _make_bare_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimDate")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "scoping_not_resolved"

def test_ready_profile_writerless_table() -> None:
    """Profile not applicable for writerless table."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.RefCurrency")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "WRITERLESS_TABLE"

def test_ready_profile_ignores_corrupt_manifest_contents() -> None:
    """Profile readiness requires manifest presence, not valid runtime JSON."""
    tmp, root = _make_project()
    with tmp:
        (root / "manifest.json").write_text("{not json", encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.ready is True
        assert result.project.reason == "ok"
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_test_gen_passes() -> None:
    """test-gen ready when profile.status is ok or partial."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_test_gen_no_profile() -> None:
    """test-gen not ready when no profile section."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["profile"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "profile_not_complete"

def test_ready_test_gen_no_target_runtime() -> None:
    """test-gen is blocked when runtime.target is missing from manifest."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"

def test_ready_test_gen_no_target_runtime_without_object() -> None:
    """test-gen fails with a target code even without an object overlay."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"
        assert result.object is None

def test_ready_test_gen_missing_target_and_sandbox_reports_target_first() -> None:
    """test-gen setup guidance reports target before sandbox when both are absent."""
    tmp, root = _make_project(include_target=False, include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"
        assert result.object is None

def test_ready_test_gen_requires_configured_sandbox_runtime() -> None:
    """test-gen is blocked when init only seeded an empty sandbox role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"

def test_ready_test_gen_no_sandbox_runtime() -> None:
    """test-gen is blocked when runtime.sandbox is missing from manifest."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"

def test_ready_test_gen_no_sandbox_runtime_without_object() -> None:
    """test-gen fails with a sandbox code even without an object overlay."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "test-gen")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"
        assert result.object is None

def test_ready_test_gen_accepts_oracle_sandbox_with_dsn() -> None:
    """test-gen treats a DSN-backed Oracle sandbox as configured."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "oracle",
            "dialect": "oracle",
            "connection": {"dsn": "localhost:1521/FREEPDB1"},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"

def test_ready_test_gen_accepts_sql_server_sandbox_without_named_env() -> None:
    """test-gen accepts a runnable SQL Server sandbox even without database/schema names."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {
                "host": "localhost",
                "port": "1433",
                "user": "sa",
                "driver": "FreeTDS",
                "password_env": "SA_PASSWORD",
            },
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "test-gen", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"

def test_ready_refactor_needs_test_gen() -> None:
    """Refactor not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "refactor", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "test_gen_not_complete"

def test_ready_refactor_passes_with_test_gen() -> None:
    """Refactor ready when test_gen.status == ok."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["test_gen"] = {"status": "ok"}
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "refactor", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_generate_with_refactor() -> None:
    """Generate ready when refactor.status == ok on proc catalog."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.project is not None
        assert result.project.reason == "ok"
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_generate_no_refactor() -> None:
    """Generate not ready when refactor missing from proc catalog."""
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "refactor_not_complete"

def test_ready_generate_no_sandbox() -> None:
    """Generate not ready when runtime.target is missing from manifest."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"

def test_ready_generate_no_sandbox_runtime() -> None:
    """Generate not ready when runtime.sandbox is missing from manifest."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"

def test_ready_generate_requires_configured_target_runtime() -> None:
    """Generate is blocked when init only seeded an empty target role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["target"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "target_not_configured"
        assert result.project.code == "TARGET_NOT_CONFIGURED"

def test_ready_generate_requires_configured_sandbox_runtime() -> None:
    """Generate is blocked when init only seeded an empty sandbox role."""
    tmp, root = _make_project()
    with tmp:
        manifest_path = root / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["runtime"]["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {},
        }
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "sandbox_not_configured"
        assert result.project.code == "SANDBOX_NOT_CONFIGURED"

def test_ready_generate_missing_dbt_project() -> None:
    """Generate not ready when dbt_project.yml is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "dbt_project.yml").unlink()
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "dbt_project_missing"
        assert result.project.code == "DBT_PROJECT_MISSING"

def test_ready_generate_missing_dbt_profile() -> None:
    """Generate not ready when profiles.yml is missing."""
    tmp, root = _make_project()
    with tmp:
        (root / "dbt" / "profiles.yml").unlink(missing_ok=True)
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "dbt_profile_missing"
        assert result.project.code == "DBT_PROFILE_MISSING"

def test_ready_generate_requires_test_gen() -> None:
    """Generate not ready when test_gen.status is absent."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        del cat["test_gen"]
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "test_gen_not_complete"
        assert result.object.code == "TEST_SPEC_MISSING"

def test_ready_generate_passes_with_both_gates() -> None:
    """Generate ready when both test_gen and refactor are ok."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_source_table() -> None:
    """Source table (is_source=True) returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "scope", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "SOURCE_TABLE"

def test_ready_seed_table() -> None:
    """Seed table returns not_applicable with SEED_TABLE code."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "SEED_TABLE"

def test_ready_excluded_table() -> None:
    """Excluded table returns not_applicable."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["excluded"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "EXCLUDED"

def test_ready_excluded_table_short_circuits_before_stage_policy() -> None:
    """Excluded table returns not_applicable before generate-stage checks."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["excluded"] = True
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "EXCLUDED"
        assert result.object.not_applicable is True

def test_ready_invalid_stage() -> None:
    """Invalid stage returns ready=False with reason=invalid_stage."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "bogus", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "invalid_stage"

def test_ready_invalid_stage_does_not_load_object_catalog() -> None:
    """Invalid stage returns project-only output without object overlay."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "manifest.json").write_text("{}", encoding="utf-8")

        result = dry_run.run_ready(root, "bogus", object_fqn="silver.DimCustomer")

        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.project is not None
        assert result.project.reason == "invalid_stage"
        assert result.object is None

def test_ready_view_scope_passes() -> None:
    """View scope ready when manifest and view catalog exist."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "scope", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_view_profile_not_scoped() -> None:
    """View profile not ready when scoping.status != analyzed."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "profile", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "scoping_not_analyzed"

def test_ready_view_profile_when_analyzed() -> None:
    """View profile ready when scoping.status == analyzed."""
    tmp, root = _make_project()
    with tmp:
        view_path = root / "catalog" / "views" / "silver.vdimsalesterritory.json"
        cat = json.loads(view_path.read_text(encoding="utf-8"))
        cat["scoping"] = {"status": "analyzed", "sql_elements": [], "logic_summary": "test"}
        view_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.vDimSalesTerritory")
        assert isinstance(result, DryRunOutput)
        assert result.ready is True
        assert result.object is not None
        assert result.object.reason == "ok"

def test_ready_setup_ddl_without_object_reports_project_only() -> None:
    """Project-only readiness should not attach an object section."""
    tmp, root = _make_project()
    with tmp:
        result = dry_run.run_ready(root, "setup-ddl")
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is None

def test_ready_generate_object_failure_preserves_project_success() -> None:
    """Object overlay should fail independently after project readiness passes."""
    tmp, root = _make_project(include_sandbox=False, include_target=False)
    with tmp:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        manifest.setdefault("runtime", {})["target"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "TargetDB"},
        }
        manifest.setdefault("runtime", {})["sandbox"] = {
            "technology": "sql_server",
            "dialect": "tsql",
            "connection": {"database": "SBX_ABC123000000"},
        }
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        (root / "dbt").mkdir(exist_ok=True)
        (root / "dbt" / "dbt_project.yml").write_text("name: test\n", encoding="utf-8")
        (root / "dbt" / "profiles.yml").write_text("test:\n  target: dev\n", encoding="utf-8")
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")
        assert result.project is not None
        assert result.project.ready is True
        assert result.object is not None
        assert result.object.ready is False
        assert result.object.reason == "refactor_not_complete"

def test_cli_ready_scope() -> None:
    """CLI ready returns JSON for object-scoped readiness."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "scope", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output["object"]["ready"] is True
        assert output["project"]["reason"] == "ok"
        assert output["object"]["reason"] == "ok"

def test_cli_ready_invalid_stage() -> None:
    """CLI ready with invalid stage still returns JSON (ready=False)."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "bogus", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["reason"] == "invalid_stage"

def test_cli_ready_project_only() -> None:
    """CLI ready supports project-only readiness without object input."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "setup-ddl", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output.get("object") is None
        assert output["project"]["reason"] == "ok"

def test_cli_ready_test_gen_missing_target_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear target setup code."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"

def test_cli_ready_test_gen_missing_sandbox_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear sandbox setup code."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"

def test_cli_ready_generate_missing_target_preserves_zero_exit() -> None:
    """CLI ready generate keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "generate", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"

def test_cli_ready_refactor_missing_sandbox_preserves_zero_exit() -> None:
    """CLI ready refactor keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "refactor", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"
