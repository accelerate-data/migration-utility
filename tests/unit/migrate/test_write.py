from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import yaml

from shared.migrate import (
    _load_refactored_sql,
    run_write,
)
from tests.unit.migrate.helpers import (
    _seed_refactor_fixture,
)


class TestRunWrite:
    """Artifact writing to dbt project."""

    def test_write_valid_sql_and_yml(self, dbt_project: Path) -> None:
        model_sql = "select 1 as id"
        schema_yml = "version: 2\nmodels:\n  - name: factsales\n"

        result = run_write(
            "silver.FactSales",
            Path("/tmp"),  # ddl_path not used by write
            dbt_project,
            model_sql,
            schema_yml,
        )

        assert result.status == "ok"
        assert len(result.written) == 2
        assert result.written == [
            "models/marts/factsales.sql",
            "models/marts/_marts__models.yml",
        ]
        assert (dbt_project / "models" / "marts" / "factsales.sql").exists()
        assert (dbt_project / "models" / "marts" / "_marts__models.yml").exists()

    def test_write_sql_only_no_yml(self, dbt_project: Path) -> None:
        result = run_write(
            "silver.FactSales",
            Path("/tmp"),
            dbt_project,
            "select 1 as id",
            "",
        )

        assert result.status == "ok"
        assert len(result.written) == 1
        assert (dbt_project / "models" / "marts" / "factsales.sql").exists()

    def test_write_snapshot_routes_to_snapshots_dir(self, dbt_project: Path) -> None:
        model_sql = "{% snapshot dim_employee_scd2 %}\nselect 1 as id\n{% endsnapshot %}"
        schema_yml = "version: 2\nsnapshots:\n  - name: dim_employee_scd2\n"

        result = run_write(
            "silver.DimEmployeeSCD2",
            Path("/tmp"),
            dbt_project,
            model_sql,
            schema_yml,
        )

        assert result.status == "ok"
        assert result.written == [
            "snapshots/dim_employee_scd2.sql",
            "snapshots/_snapshots__models.yml",
        ]
        assert (dbt_project / "snapshots" / "dim_employee_scd2.sql").exists()
        snapshot_yml = (dbt_project / "snapshots" / "_snapshots__models.yml").read_text()
        assert "snapshots:" in snapshot_yml
        assert "name: dim_employee_scd2" in snapshot_yml

    def test_write_snapshot_rejects_model_yaml_fallback(self, dbt_project: Path) -> None:
        """Snapshot YAML must use snapshots:, not silently fall back from models:."""
        model_sql = "{% snapshot dim_employee_scd2 %}\nselect 1 as id\n{% endsnapshot %}"
        schema_yml = "version: 2\nmodels:\n  - name: dim_employee_scd2\n"

        with pytest.raises(ValueError, match="Snapshot schema YAML must use top-level snapshots"):
            run_write(
                "silver.DimEmployeeSCD2",
                Path("/tmp"),
                dbt_project,
                model_sql,
                schema_yml,
            )

    def test_write_empty_sql_raises(self, dbt_project: Path) -> None:
        with pytest.raises(ValueError, match="model SQL is empty"):
            run_write("silver.FactSales", Path("/tmp"), dbt_project, "", "")

    def test_write_whitespace_sql_raises(self, dbt_project: Path) -> None:
        with pytest.raises(ValueError, match="model SQL is empty"):
            run_write("silver.FactSales", Path("/tmp"), dbt_project, "   \n  ", "")

    def test_write_nonexistent_project_raises(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "no_such_dir"
        with pytest.raises(FileNotFoundError):
            run_write("silver.FactSales", Path("/tmp"), nonexistent, "select 1", "")

    def test_write_missing_dbt_project_yml_raises(self, tmp_path: Path) -> None:
        """Directory exists but no dbt_project.yml."""
        empty_dir = tmp_path / "empty_dbt"
        empty_dir.mkdir()
        with pytest.raises(FileNotFoundError):
            run_write("silver.FactSales", Path("/tmp"), empty_dir, "select 1", "")

    def test_write_idempotent(self, dbt_project: Path) -> None:
        """Running write twice produces the same files."""
        model_sql = "select 1 as id"
        schema_yml = "version: 2\n"

        run_write("silver.FactSales", Path("/tmp"), dbt_project, model_sql, schema_yml)
        run_write("silver.FactSales", Path("/tmp"), dbt_project, model_sql, schema_yml)

        sql_file = dbt_project / "models" / "marts" / "factsales.sql"
        assert sql_file.read_text() == model_sql

    def test_write_parallel_model_yaml_preserves_sibling_entries(
        self,
        dbt_project: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Concurrent mart writes merge into shared YAML without dropping siblings."""
        from shared.migrate_support import artifacts

        original_atomic_write = artifacts._atomic_write

        def delayed_atomic_write(path: Path, content: str) -> None:
            if path.name == "_marts__models.yml":
                time.sleep(0.05)
            original_atomic_write(path, content)

        monkeypatch.setattr(artifacts, "_atomic_write", delayed_atomic_write)

        writes = [
            (
                "silver.FactSales",
                "select 1 as fact_id",
                "version: 2\nmodels:\n  - name: factsales\n",
            ),
            (
                "silver.DimCustomer",
                "select 1 as customer_id",
                "version: 2\nmodels:\n  - name: dimcustomer\n",
            ),
        ]

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(run_write, table, Path("/tmp"), dbt_project, sql, yml)
                for table, sql, yml in writes
            ]
            for future in futures:
                future.result()

        schema = yaml.safe_load(
            (dbt_project / "models" / "marts" / "_marts__models.yml").read_text(
                encoding="utf-8"
            )
        )
        model_names = {model["name"] for model in schema["models"]}
        assert model_names == {"factsales", "dimcustomer"}

    def test_write_creates_marts_dir_if_missing(self, tmp_path: Path) -> None:
        """Write creates models/marts/ if it doesn't exist."""
        dbt = tmp_path / "dbt_no_staging"
        dbt.mkdir()
        (dbt / "dbt_project.yml").write_text("name: test\n")

        result = run_write("silver.FactSales", Path("/tmp"), dbt, "select 1", "")
        assert result.status == "ok"
        assert (dbt / "models" / "marts" / "factsales.sql").exists()

    def test_write_nonexistent_project_exits_2(self, tmp_path: Path) -> None:
        """CLI write to nonexistent dbt project exits with code 2."""
        from typer.testing import CliRunner

        from shared.migrate import app

        runner = CliRunner()
        nonexistent = tmp_path / "no_such_dir"
        result = runner.invoke(app, [
            "write",
            "--table", "silver.FactSales",
            "--dbt-project-path", str(nonexistent),
            "--model-sql", "select 1",
        ])
        assert result.exit_code == 2

def test_write_does_not_read_catalog(tmp_path: Path) -> None:
    """write only writes dbt artifacts — corrupt catalog does not affect it."""
    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text("name: test\nversion: '1.0.0'\nconfig-version: 2\n")
    (dbt / "models" / "marts").mkdir(parents=True)
    result = run_write("silver.FactSales", Path("/tmp"), dbt, "select 1", "")
    assert result.status == "ok"

def test_load_refactored_sql_returns_sql_when_status_ok(tmp_path: Path) -> None:
    """Procedure catalog with refactor.status=ok returns refactored_sql."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", {"status": "ok", "refactored_sql": "SELECT 1"})
    assert _load_refactored_sql(tmp_path, "silver.mytable") == "SELECT 1"

def test_load_refactored_sql_returns_none_when_refactor_section_absent(tmp_path: Path) -> None:
    """Procedure catalog without refactor section returns None."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", None)
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None

def test_load_refactored_sql_returns_none_when_refactored_sql_field_absent(tmp_path: Path) -> None:
    """Refactor section present but refactored_sql field missing returns None."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", {"status": "partial"})
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None

def test_load_refactored_sql_returns_none_when_no_selected_writer(tmp_path: Path) -> None:
    """Table catalog with no selected_writer returns None."""
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    (tmp_path / "manifest.json").write_text("{}")
    (table_dir / "silver.mytable.json").write_text(json.dumps({"schema": "silver", "name": "mytable"}))
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None
