from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared import dry_run
from tests.unit.dry_run.dry_run_test_helpers import (
    _cli_runner,
    _make_reset_project,
)

def test_run_reset_migration_profile_clears_downstream_and_preserves_scoping(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "profile", ["silver.DimCustomer"])

    assert result.reset == ["silver.dimcustomer"]
    assert result.noop == []
    target = result.targets[0]
    assert target.status == "reset"
    assert "table.profile" in target.cleared_sections
    assert "table.test_gen" in target.cleared_sections
    assert "procedure:dbo.usp_load_dimcustomer.refactor" in target.cleared_sections
    assert target.deleted_files == ["test-specs/silver.dimcustomer.json"]
    assert "catalog/procedures/dbo.usp_load_dimcustomer.json" in target.mutated_files
    assert "catalog/tables/silver.dimcustomer.json" in target.mutated_files

    table_cat = json.loads((dst / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "scoping" in table_cat
    assert "profile" not in table_cat
    assert "test_gen" not in table_cat
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()

    proc_cat = json.loads((dst / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc_cat

def test_run_reset_migration_refactor_only_clears_writer_refactor(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])

    assert result.reset == ["silver.dimcustomer"]
    assert "catalog/procedures/dbo.usp_load_dimcustomer.json" in result.targets[0].mutated_files
    table_cat = json.loads((dst / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "profile" in table_cat
    assert "test_gen" in table_cat
    assert (dst / "test-specs" / "silver.dimcustomer.json").exists()

    proc_cat = json.loads((dst / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc_cat

def test_run_reset_migration_is_idempotent_noop(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])
    result = dry_run.run_reset_migration(dst, "refactor", ["silver.DimCustomer"])

    assert result.reset == []
    assert result.noop == ["silver.dimcustomer"]
    assert result.targets[0].status == "noop"

def test_run_reset_migration_multiple_tables(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(
        dst,
        "generate-tests",
        ["silver.DimCustomer", "silver.DimProduct"],
    )

    assert sorted(result.reset) == ["silver.dimcustomer", "silver.dimproduct"]
    assert result.blocked == []
    assert result.not_found == []
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()
    assert not (dst / "test-specs" / "silver.dimproduct.json").exists()

def test_run_reset_migration_blocks_model_complete_before_mutation(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    table_path = dst / "catalog" / "tables" / "silver.dimcustomer.json"
    table_cat = json.loads(table_path.read_text(encoding="utf-8"))
    table_cat["generate"] = {"status": "ok"}
    table_path.write_text(json.dumps(table_cat), encoding="utf-8")

    result = dry_run.run_reset_migration(
        dst,
        "profile",
        ["silver.DimCustomer", "silver.DimProduct"],
    )

    assert result.reset == []
    assert result.blocked == ["silver.dimcustomer"]
    assert result.targets[0].status == "blocked"

    untouched = json.loads(table_path.read_text(encoding="utf-8"))
    assert "profile" in untouched
    assert (dst / "test-specs" / "silver.dimproduct.json").exists()

def test_run_reset_migration_not_found_returns_without_mutation(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = dry_run.run_reset_migration(dst, "profile", ["silver.Missing"])

    assert result.not_found == ["silver.missing"]
    assert result.targets[0].status == "not_found"
    assert (dst / "test-specs" / "silver.dimcustomer.json").exists()

def test_run_reset_migration_mixed_valid_and_missing_resets_valid_targets(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    result = dry_run.run_reset_migration(
        dst,
        "profile",
        ["silver.DimCustomer", "silver.Missing"],
    )

    assert result.not_found == ["silver.missing"]
    assert result.reset == ["silver.dimcustomer"]
    assert {target.fqn: target.status for target in result.targets} == {
        "silver.missing": "not_found",
        "silver.dimcustomer": "reset",
    }
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()

def test_reset_migration_global_output_contract_serializes_deleted_paths(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "CLAUDE.md").write_text("# local scaffold\n", encoding="utf-8")
    (dst / ".envrc").write_text("export TEST=1\n", encoding="utf-8")
    (dst / "repo-map.json").write_text("{\"name\": \"fixture\"}\n", encoding="utf-8")
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / "ddl" / "legacy.sql").write_text("select 1;", encoding="utf-8")
    (dst / ".staging").mkdir()
    (dst / ".staging" / "state.json").write_text("{}", encoding="utf-8")
    (dst / "dbt" / "models" / "marts").mkdir(parents=True)
    (dst / "dbt" / "models" / "marts" / "dim_customer.sql").write_text(
        "select 1;", encoding="utf-8"
    )
    (dst / "dbt" / "target").mkdir(parents=True)
    (dst / "dbt" / "target" / "compiled.json").write_text("{}", encoding="utf-8")

    result = dry_run.run_reset_migration(dst, "all", [])
    payload = result.model_dump(mode="json", exclude_none=True)

    assert result.stage == "all"
    assert result.targets == []
    assert result.reset == []
    assert result.noop == []
    assert result.blocked == []
    assert result.not_found == []
    assert result.deleted_paths == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert result.missing_paths == []
    assert result.cleared_manifest_sections == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    assert payload["stage"] == "all"
    assert payload["deleted_paths"] == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert payload["missing_paths"] == []
    assert payload["cleared_manifest_sections"] == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    assert "runtime" not in manifest
    assert "extraction" not in manifest
    assert "init_handoff" not in manifest
    assert (dst / "manifest.json").exists()
    assert not (dst / "catalog").exists()
    assert not (dst / "ddl").exists()
    assert not (dst / ".staging").exists()
    assert not (dst / "test-specs").exists()
    assert not (dst / "dbt").exists()
    assert (dst / "CLAUDE.md").exists()
    assert (dst / ".envrc").exists()
    assert (dst / "repo-map.json").exists()

def test_run_reset_migration_all_reports_missing_paths_as_noop(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / ".staging").mkdir()

    result = dry_run.run_reset_migration(dst, "all", [])

    assert result.deleted_paths == ["catalog", "ddl", ".staging", "test-specs"]
    assert result.missing_paths == ["dbt"]
    assert result.cleared_manifest_sections == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]
    assert (dst / "ddl").exists() is False
    assert (dst / ".staging").exists() is False
    assert (dst / "dbt").exists() is False

def test_run_reset_migration_all_invalid_manifest_preserves_directories(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    (dst / "ddl").mkdir()
    (dst / "ddl" / "legacy.sql").write_text("select 1;", encoding="utf-8")
    (dst / "dbt" / "models").mkdir(parents=True)
    (dst / "dbt" / "models" / "model.sql").write_text("select 1;", encoding="utf-8")
    (dst / ".staging").mkdir()
    (dst / "test-specs").mkdir(exist_ok=True)
    (dst / "manifest.json").write_text("{not valid json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        dry_run.run_reset_migration(dst, "all", [])

    assert (dst / "catalog").exists()
    assert (dst / "ddl").exists()
    assert (dst / ".staging").exists()
    assert (dst / "test-specs").exists()
    assert (dst / "dbt").exists()

def test_run_reset_migration_all_rejects_extra_table_arguments(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    with pytest.raises(ValueError, match="global reset stage 'all' does not accept table arguments"):
        dry_run.run_reset_migration(dst, "all", ["silver.DimCustomer"])

def test_reset_migration_requires_at_least_one_fqn(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    with pytest.raises(ValueError, match="reset-migration requires at least one FQN for staged resets"):
        dry_run.run_reset_migration(dst, "profile", [])

def test_reset_migration_cli_all_succeeds_without_fqns(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    manifest = json.loads((dst / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (dst / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (dst / "ddl").mkdir()
    (dst / ".staging").mkdir()
    (dst / "dbt" / "models").mkdir(parents=True)

    result = _cli_runner.invoke(dry_run.app, ["reset-migration", "all", "--project-root", str(dst)])

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "all"
    assert output["deleted_paths"] == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert not (dst / "catalog").exists()
    assert not (dst / "ddl").exists()
    assert not (dst / ".staging").exists()
    assert not (dst / "test-specs").exists()
    assert not (dst / "dbt").exists()

def test_reset_migration_cli_all_rejects_extra_fqns(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)

    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "all",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 1, result.output
    output = json.loads(result.stdout)
    assert "global reset stage 'all' does not accept table arguments" in output["error"]

def test_reset_migration_cli_subcommand(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "generate-tests",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["stage"] == "generate-tests"
    assert output["reset"] == ["silver.dimcustomer"]
    assert not (dst / "test-specs" / "silver.dimcustomer.json").exists()

def test_reset_migration_cli_corrupt_catalog_exits_2(tmp_path: Path) -> None:
    dst = _make_reset_project(tmp_path)
    table_path = dst / "catalog" / "tables" / "silver.dimcustomer.json"
    table_path.write_text("{not valid json", encoding="utf-8")

    result = _cli_runner.invoke(
        dry_run.app,
        [
            "reset-migration",
            "profile",
            "--fqn",
            "silver.DimCustomer",
            "--project-root",
            str(dst),
        ],
    )

    assert result.exit_code == 2
    output = json.loads(result.stdout)
    assert "error" in output
