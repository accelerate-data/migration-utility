from __future__ import annotations

import json
from pathlib import Path

from shared.init_support.scaffold import run_scaffold_hooks, run_scaffold_project
from shared.init_support.source_config import get_source_config


def test_scaffold_project_uses_source_config_templates(tmp_path: Path) -> None:
    config = get_source_config("oracle")

    result = run_scaffold_project(tmp_path, technology="oracle")

    assert "CLAUDE.md" in result.files_created
    assert "repo-map.json" in result.files_created
    claude_md = (tmp_path / "CLAUDE.md").read_text(encoding="utf-8")
    assert claude_md == config.claude_md_fn()
    assert "## Completion Claims" in claude_md
    assert "completion-claim verification skill" in claude_md
    assert json.loads((tmp_path / "repo-map.json").read_text(encoding="utf-8")) == config.repo_map_fn()


def test_scaffold_hooks_uses_source_specific_hook(tmp_path: Path) -> None:
    result = run_scaffold_hooks(tmp_path, technology="oracle")

    assert result.hook_created is True
    hook = (tmp_path / ".githooks" / "pre-commit").read_text(encoding="utf-8")
    assert "API_KEY" in hook
    assert "SOURCE_MSSQL_PASSWORD" not in hook
