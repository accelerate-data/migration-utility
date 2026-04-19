"""Regression checks for the durable design-doc index."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()


def _load_design_index_checker():
    script_path = REPO_ROOT / "scripts" / "check_design_index.py"
    spec = importlib.util.spec_from_file_location("check_design_index", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_design_doc(root: Path, name: str) -> None:
    doc_dir = root / name
    doc_dir.mkdir(parents=True)
    (doc_dir / "README.md").write_text(f"# {name}\n", encoding="utf-8")


def test_design_index_checker_accepts_matching_index(tmp_path: Path) -> None:
    checker = _load_design_index_checker()
    design_root = tmp_path / "docs" / "design"
    design_root.mkdir(parents=True)
    _write_design_doc(design_root, "alpha")
    _write_design_doc(design_root, "beta")
    (design_root / "README.md").write_text(
        "# Design Docs\n\n"
        "## Design Index\n\n"
        "- [Alpha](alpha/README.md) — durable decision.\n"
        "- [Beta](beta/README.md) — durable decision.\n",
        encoding="utf-8",
    )

    result = checker.check_design_index(design_root)

    assert result.ok
    assert result.missing_links == []
    assert result.unindexed_dirs == []
    assert result.stale_index_dirs == []


def test_design_index_checker_reports_stale_and_missing_entries(tmp_path: Path) -> None:
    checker = _load_design_index_checker()
    design_root = tmp_path / "docs" / "design"
    design_root.mkdir(parents=True)
    _write_design_doc(design_root, "indexed")
    _write_design_doc(design_root, "unindexed")
    (design_root / "README.md").write_text(
        "# Design Docs\n\n"
        "## Design Index\n\n"
        "- [Indexed](indexed/README.md) — durable decision.\n"
        "- [Removed](removed/README.md) — stale decision.\n",
        encoding="utf-8",
    )

    result = checker.check_design_index(design_root)

    assert not result.ok
    assert result.missing_links == ["removed/README.md"]
    assert result.unindexed_dirs == ["unindexed"]
    assert result.stale_index_dirs == ["removed"]


def test_ci_runs_for_markdown_changes_and_checks_design_index() -> None:
    workflow_path = REPO_ROOT / ".github" / "workflows" / "ci.yml"
    workflow = workflow_path.read_text(encoding="utf-8")

    assert "'**/*.md'" not in workflow
    assert '"**/*.md"' not in workflow
    assert "scripts/check_design_index.py" in workflow
