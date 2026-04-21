from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
SKILL_PATH = REPO_ROOT / "skills" / "classifying-data-domains" / "SKILL.md"


def _skill_text() -> str:
    return SKILL_PATH.read_text(encoding="utf-8")


def _single_line(text: str) -> str:
    return " ".join(text.split())


def _frontmatter() -> dict[str, object]:
    text = _skill_text()
    _, frontmatter, _ = text.split("---", 2)
    return yaml.safe_load(frontmatter)


def test_skill_is_user_invocable_with_trigger_only_description() -> None:
    frontmatter = _frontmatter()

    assert frontmatter["name"] == "classifying-data-domains"
    assert frontmatter["user-invocable"] is True
    assert "warehouse-ddl" not in str(frontmatter["argument-hint"])
    assert str(frontmatter["description"]).startswith("Use when ")
    assert "Step " not in str(frontmatter["description"])


def test_skill_requires_existing_warehouse_ddl_and_does_not_accept_substitutes() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert "`warehouse-ddl/` is required" in text
    assert "Check the filesystem for the directory under the active project root before any analysis" in normalized
    assert "Do not assume it is missing without checking the concrete path" in normalized
    assert "If `warehouse-ddl/` is missing" in text
    assert "Do not create `warehouse-ddl/`" in text
    assert "partial substitutes such as pasted DDL" in text
    assert "run the warehouse DDL extraction workflow first" in text


def test_skill_resolves_paths_under_active_project_root() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert "## Project Root" in text
    assert "current working directory as the warehouse-analysis project root" in normalized
    assert "explicitly provided project root or workspace path" in normalized
    assert "otherwise, use the current working directory" in normalized.lower()
    assert "`warehouse-ddl/`" in text
    assert "`warehouse-catalog/domains/<slug>.json`" in text
    assert "warehouse-catalog/data-domains" not in text
    assert "warehouse-catalog/domain-classification" not in text
    assert "Do not write outside the active project root" in text
    assert "Use a directory listing or file listing command" in text
    assert "A failed glob" in text
    assert "not proof that `warehouse-ddl/` is missing" in normalized
    assert "run_path" not in text


def test_skill_persists_only_canonical_data_domain_files_on_request() -> None:
    text = _skill_text()

    assert "only when the user explicitly asks" in text
    assert "`warehouse-catalog/domains/<slug>.json`" in text
    assert "same accepted state serializes to the same JSON" in text
    assert "no volatile timestamps" in text
    assert "do not claim files were written unless they exist on disk" in text
    assert "`catalog/domains" not in text


def test_skill_scope_is_tables_and_views_only() -> None:
    text = _skill_text()
    normalized = _single_line(text)
    extraction_scope = text.partition("2. Extract objects from available DDL:")[2].partition(
        "3. Classify each object by dimensional modeling role.",
    )[0]

    assert "classifies only tables and views" in normalized
    assert "Do not classify procedures or functions as domain-catalog objects" in normalized
    assert "procedures" not in extraction_scope
    assert "functions" not in extraction_scope


def test_domain_file_contract_contains_only_table_and_view_objects() -> None:
    text = _skill_text()

    contract = text.partition("## Domain File Contract")[2].partition("## Persistence Rules")[0]

    assert "`objects` may contain only these keys" in contract
    assert '"tables": ["silver.fact_sales"]' in contract
    assert '"views": ["gold.vw_sales_summary"]' in contract
    assert '"procedures"' not in contract
    assert '"functions"' not in contract
    assert "Procedures and functions must not appear anywhere in persisted domain JSON" in contract


def test_skill_distinguishes_table_ownership_from_view_ownership() -> None:
    text = _skill_text()

    assert "Every table has exactly one primary functional domain" in text
    assert "A view may belong to a different functional domain than its source table" in text
    assert "Multi-domain table usage does not move table ownership" in text
    assert "a sold-opportunities view can belong to Operations" in text


def test_skill_records_cross_domain_view_dependencies_and_requires_ambiguity_handoff() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert '"cross-domain dependency"' in normalized
    assert "record a cross-domain dependency when a view depends on a table from another domain" in normalized
    assert "Ambiguous table or view ownership must be returned to the human before persistence" in normalized
    assert "recommended option first" in normalized
    assert "pick one" in normalized
    assert "is not an ownership decision" in normalized
    assert "Proceed with low confidence only when evidence is weak and there is no competing plausible primary owner" in normalized
    assert "unresolved ownership ambiguity blocks persistence" in normalized
    assert "Do not list absent example domains" in text
    assert "no visible business signal" in text


def test_skill_distinguishes_dimensional_role_from_functional_domain() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert "Dimensional role classification is separate from functional domain classification" in normalized
    assert "Role answers what kind of warehouse object it is" in normalized
    assert "functional domain answers which business area owns its meaning" in normalized


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
