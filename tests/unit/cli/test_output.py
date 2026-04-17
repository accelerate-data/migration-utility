from pathlib import Path

from shared.cli.output import summarize_repo_mutations


def test_summarize_repo_mutations_lists_root_files_individually() -> None:
    assert summarize_repo_mutations(["manifest.json", ".envrc"]) == [
        "manifest.json",
        ".envrc",
    ]


def test_summarize_repo_mutations_lists_up_to_three_files_per_directory() -> None:
    assert summarize_repo_mutations(
        [
            "catalog/tables/silver.customer.json",
            "catalog/tables/silver.product.json",
            "catalog/tables/silver.sales.json",
        ]
    ) == [
        "catalog/tables/silver.customer.json",
        "catalog/tables/silver.product.json",
        "catalog/tables/silver.sales.json",
    ]


def test_summarize_repo_mutations_collapses_more_than_three_files_per_directory() -> None:
    assert summarize_repo_mutations(
        [
            Path("ddl/tables.sql"),
            Path("ddl/procedures.sql"),
            Path("ddl/views.sql"),
            Path("ddl/functions.sql"),
        ]
    ) == ["ddl/ - 4 files"]


def test_summarize_repo_mutations_deduplicates_and_sorts_paths() -> None:
    assert summarize_repo_mutations(
        [
            "catalog/views/silver.vw_sales.json",
            "manifest.json",
            "catalog/views/silver.vw_sales.json",
            "catalog/views/silver.vw_customer.json",
        ]
    ) == [
        "manifest.json",
        "catalog/views/silver.vw_customer.json",
        "catalog/views/silver.vw_sales.json",
    ]


def test_summarize_repo_mutations_preserves_directory_markers() -> None:
    assert summarize_repo_mutations(["catalog/", "manifest.json"]) == [
        "manifest.json",
        "catalog/",
    ]
