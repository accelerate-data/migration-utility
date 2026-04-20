"""Regression checks for public-surface documentation audits."""

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


def _load_public_surface_checker():
    script_path = REPO_ROOT / "scripts" / "check_public_surface_docs.py"
    spec = importlib.util.spec_from_file_location("check_public_surface_docs", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_public_surface_checker_accepts_wiki_update_that_names_new_command() -> None:
    checker = _load_public_surface_checker()

    result = checker.audit_public_surface_docs(
        [
            checker.ChangedPath("A", "commands/new-public-command.md"),
            checker.ChangedPath("M", "docs/wiki/Command-Reference.md"),
        ],
        "",
        "Use `/new-public-command` from Claude Code.",
    )

    assert result.ok
    assert result.public_changes == ["commands/new-public-command.md"]


def test_public_surface_checker_rejects_unrelated_wiki_update() -> None:
    checker = _load_public_surface_checker()

    result = checker.audit_public_surface_docs(
        [
            checker.ChangedPath("A", "commands/new-public-command.md"),
            checker.ChangedPath("M", "docs/wiki/Command-Reference.md"),
        ],
        "",
        "This page changed, but it only documents `/existing-command`.",
    )

    assert not result.ok
    assert result.public_changes == ["commands/new-public-command.md"]
    assert result.missing_docs == {
        "commands/new-public-command.md": ["/new-public-command"]
    }


def test_public_surface_checker_rejects_new_command_without_wiki_update() -> None:
    checker = _load_public_surface_checker()

    result = checker.audit_public_surface_docs(
        [checker.ChangedPath("A", "commands/new-public-command.md")],
        "",
        "",
    )

    assert not result.ok
    assert result.public_changes == ["commands/new-public-command.md"]
    assert result.missing_docs == {
        "commands/new-public-command.md": ["/new-public-command"]
    }


def test_public_surface_checker_rejects_new_skill_without_wiki_update() -> None:
    checker = _load_public_surface_checker()

    result = checker.audit_public_surface_docs(
        [checker.ChangedPath("A", "skills/new-public-skill/SKILL.md")],
        "",
        "",
    )

    assert not result.ok
    assert result.public_changes == ["skills/new-public-skill/SKILL.md"]


def test_public_surface_checker_rejects_new_cli_registration_without_wiki_update() -> None:
    checker = _load_public_surface_checker()

    result = checker.audit_public_surface_docs(
        [checker.ChangedPath("M", "lib/shared/cli/main.py")],
        "+app.command(\"new-command\")(new_command)\n",
        "",
    )

    assert not result.ok
    assert result.public_changes == ["lib/shared/cli/main.py"]


def test_public_surface_workflow_runs_checker() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "scripts/check_public_surface_docs.py" in workflow


def test_ci_runs_version_consistency_checker() -> None:
    workflow = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "version-consistency-audit:" in workflow
    assert "scripts/check_version_consistency.py" in workflow
