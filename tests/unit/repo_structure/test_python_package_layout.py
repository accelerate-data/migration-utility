"""Regression coverage for the package split and maintainer-doc path contracts."""

from __future__ import annotations

import json
from pathlib import Path


def _find_repo_root() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "AGENTS.md").exists() and (candidate / "pytest.ini").exists():
            return candidate
    raise RuntimeError("Could not locate the repository root")


REPO_ROOT = _find_repo_root()
INTERNAL_PROJECT_PATH = "packages/ad-migration-internal"
PUBLIC_PROJECT_PATH = "packages/ad-migration-cli"
ROOT_PLUGIN_PATH = "${CLAUDE_PLUGIN_ROOT}/lib"


def test_repo_map_points_commands_at_the_split_projects() -> None:
    repo_map = json.loads((REPO_ROOT / "repo-map.json").read_text(encoding="utf-8"))

    public_cli_commands = {
        "ad_migration_setup_source": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-source",
        "ad_migration_setup_target": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-target",
        "ad_migration_setup_sandbox": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-sandbox",
        "ad_migration_teardown_sandbox": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration teardown-sandbox",
        "ad_migration_reset": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration reset",
        "ad_migration_exclude_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration exclude-table",
        "ad_migration_add_source_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration add-source-table",
    }

    internal_cli_commands = {
        "init_check_freetds": f"cd {INTERNAL_PROJECT_PATH} && uv run init check-freetds",
        "generate_sources": f"cd {INTERNAL_PROJECT_PATH} && uv run generate-sources",
        "migrate_util_status": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util status",
        "migrate_util_ready": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util ready",
        "refactor_context": f"cd {INTERNAL_PROJECT_PATH} && uv run refactor context",
        "refactor_write": f"cd {INTERNAL_PROJECT_PATH} && uv run refactor write",
        "test_harness_help": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness --help",
        "test_harness_execute_spec": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness execute-spec",
        "compare_sql": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness compare-sql",
    }

    for command_name, expected_prefix in public_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)

    for command_name, expected_prefix in internal_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)


def test_maintainer_docs_use_the_internal_project_path() -> None:
    internal_files = [
        "commands/init-ad-migration.md",
        "commands/generate-model.md",
        "commands/generate-tests.md",
        "commands/refactor.md",
        "commands/status.md",
        "skills/README.md",
        "skills/listing-objects/SKILL.md",
        "skills/analyzing-table/SKILL.md",
        "skills/analyzing-table/references/view-pipeline.md",
        "skills/analyzing-table/references/table-pipeline.md",
        "skills/analyzing-table/references/procedure-analysis.md",
        "skills/analyzing-table/references/table-writer-resolution.md",
        "skills/profiling-table/SKILL.md",
        "skills/profiling-table/references/view-classification-signals.md",
        "skills/generating-model/SKILL.md",
        "skills/generating-tests/references/command-workflow-ref.md",
        "skills/reviewing-model/SKILL.md",
        "skills/reviewing-tests/SKILL.md",
        "skills/reviewing-tests/references/review-output-contract.md",
        "skills/reviewing-tests/references/table-vs-view-context.md",
        "skills/refactoring-sql/SKILL.md",
        "skills/refactoring-sql/references/context-fields.md",
    ]

    for relative_path in internal_files:
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert ROOT_PLUGIN_PATH not in text, relative_path

    cli_testing_text = (REPO_ROOT / "docs/reference/cli-testing/README.md").read_text(
        encoding="utf-8"
    )
    assert PUBLIC_PROJECT_PATH in cli_testing_text
    assert ROOT_PLUGIN_PATH not in cli_testing_text

    init_text = (REPO_ROOT / "commands/init-ad-migration.md").read_text(encoding="utf-8")
    assert INTERNAL_PROJECT_PATH in init_text
    assert "macOS-only" in init_text or "supported only on macOS" in init_text
