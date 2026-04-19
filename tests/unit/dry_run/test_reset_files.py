"""Tests for dry-run reset filesystem helpers."""

from __future__ import annotations


def test_delete_if_present_deletes_existing_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_if_present

    path = tmp_path / "state.json"
    path.write_text("{}", encoding="utf-8")

    assert delete_if_present(path) is True
    assert not path.exists()


def test_delete_if_present_reports_missing_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_if_present

    assert delete_if_present(tmp_path / "missing.json") is False


def test_delete_tree_if_present_deletes_directory_tree(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_tree_if_present

    path = tmp_path / "dbt" / "target"
    path.mkdir(parents=True)
    (path / "compiled.json").write_text("{}", encoding="utf-8")

    assert delete_tree_if_present(tmp_path / "dbt") is True
    assert not (tmp_path / "dbt").exists()


def test_delete_tree_if_present_deletes_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_tree_if_present

    path = tmp_path / ".staging"
    path.write_text("legacy", encoding="utf-8")

    assert delete_tree_if_present(path) is True
    assert not path.exists()
