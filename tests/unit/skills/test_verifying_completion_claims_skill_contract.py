from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
SKILL_PATH = REPO_ROOT / "skills" / "verifying-completion-claims" / "SKILL.md"


def _skill_text() -> str:
    return SKILL_PATH.read_text(encoding="utf-8")


def _single_line(text: str) -> str:
    return " ".join(text.split())


def _frontmatter() -> dict[str, object]:
    text = _skill_text()
    _, frontmatter, _ = text.split("---", 2)
    return yaml.safe_load(frontmatter)


def test_skill_is_self_triggering_for_completion_claim_intent() -> None:
    frontmatter = _frontmatter()
    description = str(frontmatter["description"])

    assert frontmatter["name"] == "verifying-completion-claims"
    assert frontmatter["user-invocable"] is False
    assert "completion" in description
    assert "successful" in description
    assert "passing" in description
    assert "PR-ready" in description
    assert "stage-complete" in description
    assert "slash command" not in description.lower()


def test_skill_requires_fresh_evidence_loop_before_claim_wording() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert "Identify the exact claim" in text
    assert "Determine the minimum fresh evidence" in text
    assert "Inspect that evidence directly" in text
    assert "Compare the evidence to the intended wording" in text
    assert "verified" in text
    assert "downgraded" in text
    assert "blocked" in text
    assert "confidence" in normalized
    assert "expected side effects" in normalized


def test_skill_does_not_trust_sub_agent_success_reports_alone() -> None:
    text = _skill_text()
    normalized = _single_line(text)

    assert "Sub-agent reports are not sufficient evidence" in text
    assert "inspect" in normalized
    assert "artifact" in normalized
    assert "command output" in normalized
    assert "PR state" in normalized
