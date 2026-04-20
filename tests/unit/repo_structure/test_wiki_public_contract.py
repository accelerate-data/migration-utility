"""Regression coverage for public wiki command and artifact wording."""

from __future__ import annotations

import re
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()
WIKI_DIR = REPO_ROOT / "docs" / "wiki"


def _wiki_text() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(WIKI_DIR.glob("*.md"))
    )


def test_wiki_uses_public_command_names_after_rename() -> None:
    text = _wiki_text()

    assert not re.search(r"(?<![A-Za-z0-9_-])/(scope|profile|refactor)(?![A-Za-z0-9_-])", text)
    assert "/scope-tables" in text
    assert "/profile-tables" in text
    assert "/refactor-query" in text


def test_wiki_documents_whole_mart_commands() -> None:
    home = (WIKI_DIR / "Home.md").read_text(encoding="utf-8")
    command_reference = (WIKI_DIR / "Command-Reference.md").read_text(encoding="utf-8")
    whole_mart = (WIKI_DIR / "Whole-Mart-Migration.md").read_text(
        encoding="utf-8"
    )
    text = _wiki_text()

    assert "[[Whole-Mart Migration]]" in home
    assert "## Flow" in whole_mart
    assert "`ad-migration setup-source`" in whole_mart
    assert "`/scope-tables`" in whole_mart
    assert "`ad-migration setup-target`" in whole_mart
    assert "`ad-migration setup-sandbox`" in whole_mart
    assert "`/migrate-mart-plan`" in whole_mart
    assert "`/migrate-mart <plan-file>`" in whole_mart
    assert "[[Command Migrate Mart Plan]]" in whole_mart
    assert "[[Command Migrate Mart]]" in whole_mart
    assert "/migrate-mart-plan" in command_reference
    assert "/migrate-mart" in command_reference
    assert "[`/migrate-mart-plan`](Command-Migrate-Mart-Plan)" in command_reference
    assert "[`/migrate-mart`](Command-Migrate-Mart)" in command_reference
    assert "final coordinator PR" in text


def test_wiki_sidebar_surfaces_whole_mart_workflow() -> None:
    sidebar = (WIKI_DIR / "_Sidebar.md").read_text(encoding="utf-8")

    assert "**Whole-Mart Migration**" in sidebar
    assert "- [[Whole-Mart Migration]]" in sidebar
    assert "- [[Command Migrate Mart Plan]]" in sidebar
    assert "- [[Command Migrate Mart]]" in sidebar


def test_wiki_documents_whole_mart_command_pages() -> None:
    plan_page = (WIKI_DIR / "Command-Migrate-Mart-Plan.md").read_text(
        encoding="utf-8"
    )
    execute_page = (WIKI_DIR / "Command-Migrate-Mart.md").read_text(
        encoding="utf-8"
    )

    assert "# Command: /migrate-mart-plan" in plan_page
    assert "## Gates" in plan_page
    assert "`runtime.source`" in plan_page
    assert "`runtime.target`" in plan_page
    assert "`runtime.sandbox`" in plan_page
    assert "`dbt/dbt_project.yml`" in plan_page
    assert "source, seed, and excluded" in plan_page

    assert "# Command: /migrate-mart" in execute_page
    assert "## Failure recovery" in execute_page
    assert "first incomplete stage" in execute_page
    assert "final coordinator PR" in execute_page


def test_wiki_does_not_document_removed_test_spec_yaml_artifacts() -> None:
    text = _wiki_text()

    assert "test-specs/<item_id>.yml" not in text
    assert "test-specs/*.yml" not in text
    assert "dbt-ready YAML test artifacts" not in text


def test_wiki_uses_public_workflow_language_not_internal_skills() -> None:
    text = _wiki_text().lower()

    assert " skill " not in text
    assert " skill." not in text
    assert " skill," not in text
    assert " skill)" not in text


def test_wiki_does_not_expose_internal_cli_or_implementation_paths() -> None:
    text = _wiki_text()

    internal_terms = [
        "migrate-util",
        "test-harness",
        "setup-ddl",
        "catalog-enrich",
        "generate-sources",
        "uv run --project",
        "packages/ad-migration-internal",
        "skills/",
        "commands/",
        "scripts/",
        "stage-worktree.sh",
        "stage-pr.sh",
    ]
    for term in internal_terms:
        assert term not in text


def test_wiki_uses_renamed_command_examples() -> None:
    text = _wiki_text()

    assert "scope:" not in text
    assert "profile:" not in text
    assert "refactor:" not in text
    assert "scope(" not in text
    assert "profile(" not in text
    assert "refactor(" not in text


def test_wiki_documents_linux_wsl_platform_contract() -> None:
    text = _wiki_text()
    cli_reference = (WIKI_DIR / "CLI-Reference.md").read_text(encoding="utf-8")

    assert "macOS, Linux, and WSL" in text
    assert "Native Windows is not supported" in text
    assert "Use WSL" in text
    assert "platform package manager" in text
    assert "Linux or WSL" in cli_reference
    assert "brew install ad-migration" in cli_reference
    assert "release wheel artifacts" in cli_reference
