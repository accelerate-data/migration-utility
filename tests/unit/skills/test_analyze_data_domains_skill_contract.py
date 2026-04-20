from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
SKILL_PATH = REPO_ROOT / "skills" / "analyze-data-domains" / "SKILL.md"


def _skill_text() -> str:
    return SKILL_PATH.read_text(encoding="utf-8")


def _frontmatter() -> dict[str, object]:
    text = _skill_text()
    _, frontmatter, _ = text.split("---", 2)
    return yaml.safe_load(frontmatter)


def test_skill_is_user_invocable_with_trigger_only_description() -> None:
    frontmatter = _frontmatter()

    assert frontmatter["name"] == "analyze-data-domains"
    assert frontmatter["user-invocable"] is True
    assert "warehouse-ddl" in str(frontmatter["argument-hint"])
    assert str(frontmatter["description"]).startswith("Use when ")
    assert "Step " not in str(frontmatter["description"])


def test_skill_requires_existing_warehouse_ddl_and_does_not_accept_substitutes() -> None:
    text = _skill_text()

    assert "`warehouse-ddl/` is required" in text
    assert "If `warehouse-ddl/` is missing" in text
    assert "Do not create `warehouse-ddl/`" in text
    assert "Do not create `warehouse-catalog/`" in text
    assert "Do not accept pasted DDL" in text
    assert "run the warehouse DDL extraction workflow first" in text


def test_skill_persists_only_canonical_data_domain_files_on_request() -> None:
    text = _skill_text()

    assert "only when the user explicitly asks" in text
    assert "`warehouse-catalog/data-domains/<slug>.json`" in text
    assert "same accepted state serializes to the same JSON" in text
    assert "no volatile timestamps" in text
    assert "`catalog/data-domains" not in text


def test_referenced_files_exist_and_dependency_direction_is_consistent() -> None:
    text = _skill_text()

    for relative_path in [
        "references/22_dw_table_patterns.md",
        "references/21_domain_taxonomy.md",
    ]:
        assert relative_path in text
        assert (SKILL_PATH.parent / relative_path).exists()

    assert "references/dw_table_patterns.md" not in text
    assert "references/domain_taxonomy.md" not in text
    assert "A references B" in text
    assert "A depends on B" in text
    assert "no upstream dependencies" in text
    assert "no incoming dependencies" not in text
