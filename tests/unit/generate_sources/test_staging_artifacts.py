"""Tests for generated staging source artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from shared.generate_sources_support.staging import write_staging_artifacts
from shared.output_models.generate_sources import GenerateSourcesOutput


def _result_with_sources(tables: list[dict[str, Any]]) -> GenerateSourcesOutput:
    return GenerateSourcesOutput(
        sources={"version": 2, "sources": [{"name": "bronze", "tables": tables}]},
        included=[f"bronze.{str(table['name']).lower()}" for table in tables],
        excluded=[],
        unconfirmed=[],
        incomplete=[],
    )


def _result_without_sources(**updates: object) -> GenerateSourcesOutput:
    values = {
        "sources": None,
        "included": [],
        "excluded": [],
        "unconfirmed": [],
        "incomplete": [],
    }
    values.update(updates)
    return GenerateSourcesOutput(**values)


def _read_staging_models(project_root: Path) -> dict[str, Any]:
    staging_models_path = project_root / "dbt" / "models" / "staging" / "_staging__models.yml"
    return yaml.safe_load(staging_models_path.read_text(encoding="utf-8"))


def test_staging_support_module_exports_artifact_writer() -> None:
    assert callable(write_staging_artifacts)


def test_write_staging_artifacts_writes_sources_models_and_wrappers(tmp_path: Path) -> None:
    result = _result_with_sources([
        {
            "name": "Customer",
            "columns": [
                {"name": "customer_id", "data_type": "INT", "tests": ["not_null"]},
            ],
        }
    ])

    written = write_staging_artifacts(tmp_path, result)

    staging_dir = tmp_path / "dbt" / "models" / "staging"
    assert written.path == str(staging_dir / "_staging__sources.yml")
    assert written.written_paths == [
        "dbt/models/staging/_staging__sources.yml",
        "dbt/models/staging/_staging__models.yml",
        "dbt/models/staging/stg_bronze__customer.sql",
    ]
    assert (staging_dir / "stg_bronze__customer.sql").read_text(encoding="utf-8").startswith("with\n")
    models = _read_staging_models(tmp_path)
    assert models["models"][0]["name"] == "stg_bronze__customer"
    assert models["models"][0]["columns"] == [{"name": "customer_id", "data_type": "INT"}]
    assert models["unit_tests"][0]["model"] == "stg_bronze__customer"


def test_write_staging_artifacts_writes_multiple_wrappers_and_models(tmp_path: Path) -> None:
    result = _result_with_sources([
        {
            "name": "Customer",
            "columns": [
                {"name": "customer_id", "data_type": "INT"},
                {"name": "loaded_at", "data_type": "DATETIME2"},
            ],
        },
        {
            "name": "Order",
            "columns": [
                {"name": "order_id", "data_type": "INT"},
                {"name": "loaded_at", "data_type": "DATETIME2"},
            ],
        },
    ])
    dbt_dir = tmp_path / "dbt"
    (dbt_dir / "models" / "staging").mkdir(parents=True)

    written = write_staging_artifacts(tmp_path, result)

    staging_models_path = dbt_dir / "models" / "staging" / "_staging__models.yml"
    customer_wrapper_path = dbt_dir / "models" / "staging" / "stg_bronze__customer.sql"
    order_wrapper_path = dbt_dir / "models" / "staging" / "stg_bronze__order.sql"
    assert "dbt/models/staging/_staging__models.yml" in written.written_paths
    assert "dbt/models/staging/stg_bronze__customer.sql" in written.written_paths
    assert "dbt/models/staging/stg_bronze__order.sql" in written.written_paths
    assert customer_wrapper_path.read_text(encoding="utf-8") == (
        "with\n"
        "\n"
        "source as (\n"
        "\n"
        "    select * from {{ source('bronze', 'Customer') }}\n"
        "\n"
        "),\n"
        "\n"
        "final as (\n"
        "\n"
        "    select * from source\n"
        "\n"
        ")\n"
        "\n"
        "select * from final\n"
    )
    assert order_wrapper_path.read_text(encoding="utf-8") == (
        "with\n"
        "\n"
        "source as (\n"
        "\n"
        "    select * from {{ source('bronze', 'Order') }}\n"
        "\n"
        "),\n"
        "\n"
        "final as (\n"
        "\n"
        "    select * from source\n"
        "\n"
        ")\n"
        "\n"
        "select * from final\n"
    )
    staging_models_content = staging_models_path.read_text(encoding="utf-8")
    assert "name: stg_bronze__customer" in staging_models_content
    assert "name: stg_bronze__order" in staging_models_content


def test_write_staging_artifacts_adds_contracts_and_baseline_unit_tests(tmp_path: Path) -> None:
    result = _result_with_sources([
        {
            "name": "Customer",
            "columns": [
                {"name": "customer_id", "data_type": "INT"},
                {"name": "email", "data_type": "NVARCHAR(255)"},
                {"name": "loaded_at", "data_type": "DATETIME2"},
            ],
        }
    ])

    written = write_staging_artifacts(tmp_path, result)

    staging_models = _read_staging_models(tmp_path)
    assert written.error is None
    assert written.generated_model_names == ["stg_bronze__customer"]
    assert written.generated_source_selectors == ["source:bronze.Customer"]
    assert staging_models["models"] == [
        {
            "name": "stg_bronze__customer",
            "description": "Pass-through staging wrapper for bronze.Customer",
            "config": {"contract": {"enforced": True}},
            "columns": [
                {"name": "customer_id", "data_type": "INT"},
                {"name": "email", "data_type": "NVARCHAR(255)"},
                {"name": "loaded_at", "data_type": "DATETIME2"},
            ],
        }
    ]
    assert staging_models["unit_tests"] == [
        {
            "name": "test_stg_bronze__customer_passthrough",
            "model": "stg_bronze__customer",
            "given": [
                {
                    "input": "source('bronze', 'Customer')",
                    "rows": [
                        {
                            "customer_id": 1,
                            "email": "sample_email",
                            "loaded_at": "2020-01-01 00:00:00",
                        }
                    ],
                }
            ],
            "expect": {
                "rows": [
                    {
                        "customer_id": 1,
                        "email": "sample_email",
                        "loaded_at": "2020-01-01 00:00:00",
                    }
                ]
            },
        }
    ]


def test_write_staging_artifacts_removes_stale_artifacts_when_sources_are_none(
    tmp_path: Path,
) -> None:
    staging_dir = tmp_path / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)
    stale_sources = staging_dir / "_staging__sources.yml"
    stale_models = staging_dir / "_staging__models.yml"
    stale_wrapper = staging_dir / "stg_bronze__customer.sql"
    stale_sources.write_text("version: 2\nsources: []\n", encoding="utf-8")
    stale_models.write_text("version: 2\nmodels: []\n", encoding="utf-8")
    stale_wrapper.write_text("select 1\n", encoding="utf-8")
    result = _result_without_sources(
        error="STAGING_CONTRACT_TYPE_MISSING",
        message="Cannot generate staging contract",
    )

    written = write_staging_artifacts(tmp_path, result)

    assert written.sources is None
    assert written.error == "STAGING_CONTRACT_TYPE_MISSING"
    assert written.message == "Cannot generate staging contract"
    assert "dbt/models/staging/_staging__sources.yml" in written.written_paths
    assert "dbt/models/staging/_staging__models.yml" in written.written_paths
    assert "dbt/models/staging/stg_bronze__customer.sql" in written.written_paths
    assert not stale_sources.exists()
    assert not stale_models.exists()
    assert not stale_wrapper.exists()


def test_write_staging_artifacts_does_not_copy_source_tests_to_staging_models(
    tmp_path: Path,
) -> None:
    result = _result_with_sources([
        {
            "name": "Customer",
            "columns": [
                {"name": "customer_id", "data_type": "INT", "tests": ["not_null", "unique"]},
                {"name": "email", "data_type": "NVARCHAR(255)"},
            ],
        }
    ])

    written = write_staging_artifacts(tmp_path, result)

    assert written.path is not None
    staging_models = _read_staging_models(tmp_path)
    staging_columns = staging_models["models"][0]["columns"]
    assert staging_columns == [
        {"name": "customer_id", "data_type": "INT"},
        {"name": "email", "data_type": "NVARCHAR(255)"},
    ]


def test_write_staging_artifacts_removes_stale_wrappers(tmp_path: Path) -> None:
    first_result = _result_with_sources([
        {"name": "Customer"},
        {"name": "Order"},
    ])
    second_result = _result_with_sources([{"name": "Customer"}])
    staging_dir = tmp_path / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)

    write_staging_artifacts(tmp_path, first_result)
    customer_wrapper_path = staging_dir / "stg_bronze__customer.sql"
    order_wrapper_path = staging_dir / "stg_bronze__order.sql"
    assert customer_wrapper_path.exists()
    assert order_wrapper_path.exists()

    write_staging_artifacts(tmp_path, second_result)
    assert customer_wrapper_path.exists()
    assert not order_wrapper_path.exists()
    staging_models = _read_staging_models(tmp_path)
    assert [model["name"] for model in staging_models["models"]] == ["stg_bronze__customer"]
    assert [test["model"] for test in staging_models["unit_tests"]] == ["stg_bronze__customer"]


def test_write_staging_artifacts_removes_artifacts_when_no_sources_remain(tmp_path: Path) -> None:
    first_result = _result_with_sources([{"name": "Customer"}])
    second_result = _result_without_sources()
    staging_dir = tmp_path / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)

    first = write_staging_artifacts(tmp_path, first_result)
    assert first.path is not None
    assert (staging_dir / "_staging__sources.yml").exists()
    assert (staging_dir / "_staging__models.yml").exists()
    assert (staging_dir / "stg_bronze__customer.sql").exists()

    second = write_staging_artifacts(tmp_path, second_result)
    assert second.path is None
    assert "dbt/models/staging/_staging__sources.yml" in second.written_paths
    assert "dbt/models/staging/_staging__models.yml" in second.written_paths
    assert "dbt/models/staging/stg_bronze__customer.sql" in second.written_paths
    assert not (staging_dir / "_staging__sources.yml").exists()
    assert not (staging_dir / "_staging__models.yml").exists()
    assert not (staging_dir / "stg_bronze__customer.sql").exists()
