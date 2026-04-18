from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.runtime_config_models import RuntimeConnection, RuntimeRole, RuntimeSchemas


def _write_project(project_root: Path) -> None:
    (project_root / "catalog" / "tables").mkdir(parents=True)
    manifest = {
        "schema_version": "1.0",
        "technology": "sql_server",
        "dialect": "tsql",
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SourceDB", "schema": "silver"},
            },
            "target": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "TargetDB", "password_env": "TARGET_MSSQL_PASSWORD"},
                "schemas": {"source": "bronze"},
            },
        },
    }
    (project_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    _write_table(project_root, "silver", "DimCustomer")
    _write_table(project_root, "silver", "FactSales")
    _write_table(project_root, "silver", "Excluded", excluded=True)
    _write_table(project_root, "silver", "ProcedureTarget", is_source=False)


def _write_table(
    project_root: Path,
    schema: str,
    name: str,
    *,
    is_source: bool = True,
    excluded: bool = False,
) -> None:
    payload = {
        "schema": schema,
        "name": name,
        "is_source": is_source,
        "excluded": excluded,
        "columns": [
            {"name": "id", "sql_type": "INT", "is_nullable": False},
            {"name": "name", "sql_type": "NVARCHAR(50)", "is_nullable": True},
        ],
    }
    (project_root / "catalog" / "tables" / f"{schema.lower()}.{name.lower()}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


class RecordingAdapter:
    def __init__(self, role: RuntimeRole, project_root: Path | None = None) -> None:
        self.role = role
        self.project_root = project_root
        self.fetch_results: dict[str, tuple[list[str], list[tuple[object, ...]]]] = {}
        self.failures: set[str] = set()
        self.calls: list[tuple[object, ...]] = []

    @classmethod
    def from_role(cls, role: RuntimeRole, *, project_root: Path | None = None) -> "RecordingAdapter":
        return cls(role, project_root)

    def fetch_source_rows(
        self,
        schema_name: str,
        table_name: str,
        *,
        limit: int,
        predicate: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        key = f"{schema_name.lower()}.{table_name.lower()}"
        self.calls.append(("fetch", schema_name, table_name, limit, predicate, columns))
        if key in self.failures:
            raise ValueError(f"source failed for {key}")
        return self.fetch_results.get(key, (["id", "name"], [(1, "Alice")]))

    def truncate_table(self, schema_name: str, table_name: str) -> None:
        self.calls.append(("truncate", schema_name, table_name))

    def insert_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: list[str],
        rows: list[tuple[object, ...]],
    ) -> int:
        self.calls.append(("insert", schema_name, table_name, columns, rows))
        return len(rows)


def test_dry_run_plans_confirmed_source_tables_without_copying(tmp_path: Path):
    from shared.replicate_source_tables import run_replicate_source_tables

    _write_project(tmp_path)
    source = RecordingAdapter(
        RuntimeRole(technology="sql_server", dialect="tsql", connection=RuntimeConnection(database="SourceDB"))
    )
    target = RecordingAdapter(
        RuntimeRole(
            technology="sql_server",
            dialect="tsql",
            connection=RuntimeConnection(database="TargetDB"),
            schemas=RuntimeSchemas(source="bronze"),
        )
    )

    result = run_replicate_source_tables(
        tmp_path,
        limit=100,
        dry_run=True,
        source_adapter=source,
        target_adapter=target,
    )

    assert result.status == "ok"
    assert result.dry_run is True
    assert [table.fqn for table in result.tables] == ["silver.dimcustomer", "silver.factsales"]
    assert [table.target_schema for table in result.tables] == ["bronze", "bronze"]
    assert source.calls == []
    assert target.calls == []


def test_select_exclude_and_filters_shape_execution_plan(tmp_path: Path):
    from shared.replicate_source_tables import run_replicate_source_tables

    _write_project(tmp_path)
    source = RecordingAdapter(
        RuntimeRole(technology="sql_server", dialect="tsql", connection=RuntimeConnection(database="SourceDB"))
    )
    target = RecordingAdapter(
        RuntimeRole(technology="sql_server", dialect="tsql", connection=RuntimeConnection(database="TargetDB"))
    )

    result = run_replicate_source_tables(
        tmp_path,
        limit=25,
        select=["silver.DimCustomer", "silver.FactSales"],
        exclude=["silver.FactSales"],
        filters=["silver.DimCustomer=id >= 10"],
        source_adapter=source,
        target_adapter=target,
    )

    assert result.status == "ok"
    assert [table.fqn for table in result.tables] == ["silver.dimcustomer"]
    assert result.tables[0].predicate == "id >= 10"
    assert source.calls == [("fetch", "silver", "DimCustomer", 25, "id >= 10", ["id", "name"])]
    assert target.calls[0] == ("truncate", "bronze", "DimCustomer")
    assert target.calls[1][0:4] == ("insert", "bronze", "DimCustomer", ["id", "name"])


def test_multi_table_run_continues_after_per_table_failure(tmp_path: Path):
    from shared.replicate_source_tables import run_replicate_source_tables

    _write_project(tmp_path)
    source = RecordingAdapter(
        RuntimeRole(technology="sql_server", dialect="tsql", connection=RuntimeConnection(database="SourceDB"))
    )
    target = RecordingAdapter(
        RuntimeRole(technology="sql_server", dialect="tsql", connection=RuntimeConnection(database="TargetDB"))
    )
    source.failures.add("silver.dimcustomer")

    result = run_replicate_source_tables(
        tmp_path,
        limit=10,
        source_adapter=source,
        target_adapter=target,
    )

    assert result.status == "error"
    assert [(table.fqn, table.status) for table in result.tables] == [
        ("silver.dimcustomer", "error"),
        ("silver.factsales", "ok"),
    ]
    assert "source failed" in (result.tables[0].error or "")
    assert ("fetch", "silver", "FactSales", 10, None, ["id", "name"]) in source.calls


def test_rejects_invalid_limits_and_filter_targets(tmp_path: Path):
    from shared.replicate_source_tables import run_replicate_source_tables

    _write_project(tmp_path)
    with pytest.raises(ValueError, match="LIMIT_TOO_HIGH"):
        run_replicate_source_tables(tmp_path, limit=10001)

    with pytest.raises(ValueError, match="FILTER_TABLE_NOT_SELECTED"):
        run_replicate_source_tables(
            tmp_path,
            limit=10,
            select=["silver.DimCustomer"],
            filters=["silver.FactSales=id > 0"],
        )
