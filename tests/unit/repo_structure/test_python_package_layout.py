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


def test_repo_map_declares_the_split_python_projects() -> None:
    repo_map = json.loads((REPO_ROOT / "repo-map.json").read_text(encoding="utf-8"))

    package_managers = {
        entry["name"]: {
            "lockfile": entry["lockfile"],
            "manifest": entry["manifest"],
        }
        for entry in repo_map["package_managers"]
    }

    assert package_managers["uv (shared)"] == {
        "lockfile": "lib/uv.lock",
        "manifest": "lib/pyproject.toml",
    }
    assert package_managers["uv (public cli)"] == {
        "lockfile": "packages/ad-migration-cli/uv.lock",
        "manifest": "packages/ad-migration-cli/pyproject.toml",
    }
    assert package_managers["uv (internal cli)"] == {
        "lockfile": "packages/ad-migration-internal/uv.lock",
        "manifest": "packages/ad-migration-internal/pyproject.toml",
    }
    assert package_managers["uv (ddl-mcp)"] == {
        "lockfile": "mcp/ddl/uv.lock",
        "manifest": "mcp/ddl/pyproject.toml",
    }


def test_release_workflow_exists() -> None:
    assert (REPO_ROOT / ".github" / "workflows" / "release-cli.yml").is_file()


def test_repo_map_points_commands_at_the_split_projects() -> None:
    repo_map = json.loads((REPO_ROOT / "repo-map.json").read_text(encoding="utf-8"))

    public_cli_commands = {
        "ad_migration_setup_source": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-source",
        "ad_migration_setup_target": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-target",
        "ad_migration_setup_sandbox": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration setup-sandbox",
        "ad_migration_teardown_sandbox": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration teardown-sandbox",
        "ad_migration_doctor_drivers": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration doctor drivers",
        "ad_migration_reset": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration reset",
        "ad_migration_exclude_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration exclude-table",
        "ad_migration_add_source_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration add-source-table",
    }

    internal_cli_commands = {
        "catalog_enrich": f"cd {INTERNAL_PROJECT_PATH} && uv run catalog-enrich",
        "discover_list": f"cd {INTERNAL_PROJECT_PATH} && uv run discover list",
        "discover_show": f"cd {INTERNAL_PROJECT_PATH} && uv run discover show",
        "discover_refs": f"cd {INTERNAL_PROJECT_PATH} && uv run discover refs",
        "discover_write_scoping": f"cd {INTERNAL_PROJECT_PATH} && uv run discover write-scoping",
        "discover_write_slice": f"cd {INTERNAL_PROJECT_PATH} && uv run discover write-slice",
        "discover_write_statements": f"cd {INTERNAL_PROJECT_PATH} && uv run discover write-statements",
        "init_check_freetds": f"cd {INTERNAL_PROJECT_PATH} && uv run init check-freetds",
        "init_discover_mssql_driver_override": f"cd {INTERNAL_PROJECT_PATH} && uv run init discover-mssql-driver-override",
        "init_scaffold_project": f"cd {INTERNAL_PROJECT_PATH} && uv run init scaffold-project",
        "init_scaffold_hooks": f"cd {INTERNAL_PROJECT_PATH} && uv run init scaffold-hooks",
        "init_write_local_env_overrides": f"cd {INTERNAL_PROJECT_PATH} && uv run init write-local-env-overrides",
        "generate_sources": f"cd {INTERNAL_PROJECT_PATH} && uv run generate-sources",
        "migrate_context": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate context",
        "migrate_render_unit_tests": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate render-unit-tests",
        "migrate_write": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate write",
        "migrate_write_catalog": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate write-catalog",
        "migrate_util_status": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util status",
        "migrate_util_ready": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util ready",
        "migrate_util_exclude": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util exclude",
        "migrate_util_reset_migration": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util reset-migration",
        "migrate_util_sync_excluded_warnings": f"cd {INTERNAL_PROJECT_PATH} && uv run migrate-util sync-excluded-warnings",
        "refactor_context": f"cd {INTERNAL_PROJECT_PATH} && uv run refactor context",
        "refactor_write": f"cd {INTERNAL_PROJECT_PATH} && uv run refactor write",
        "profile_view_context": f"cd {INTERNAL_PROJECT_PATH} && uv run profile view-context",
        "profile_context": f"cd {INTERNAL_PROJECT_PATH} && uv run profile context",
        "profile_write": f"cd {INTERNAL_PROJECT_PATH} && uv run profile write",
        "setup_ddl_write_partial_manifest": f"cd {INTERNAL_PROJECT_PATH} && uv run setup-ddl write-partial-manifest",
        "test_harness_help": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness --help",
        "test_harness_execute_spec": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness execute-spec",
        "test_harness_sandbox_status": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness sandbox-status",
        "test_harness_validate_review": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness validate-review",
        "test_harness_write": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness write",
        "compare_sql": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness compare-sql",
        "test_harness_compare_sql": f"cd {INTERNAL_PROJECT_PATH} && uv run test-harness compare-sql",
    }

    expected_public_command_names = set(public_cli_commands)
    expected_internal_command_names = set(internal_cli_commands)

    assert expected_public_command_names.issubset(repo_map["commands"])
    assert expected_internal_command_names.issubset(repo_map["commands"])

    for command_name, expected_prefix in public_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)

    for command_name, expected_prefix in internal_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)


def test_maintainer_docs_use_the_internal_project_path() -> None:
    internal_files = [
        "commands/init-ad-migration.md",
        "commands/generate-model.md",
        "commands/generate-tests.md",
        "commands/refactor-query.md",
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


def test_init_command_runs_public_driver_doctor_and_keeps_internal_checks() -> None:
    init_text = (REPO_ROOT / "commands/init-ad-migration.md").read_text(encoding="utf-8")
    public_doctor = "ad-migration doctor drivers --project-root . --json"
    internal_pyproject = (
        REPO_ROOT / "packages/ad-migration-internal/pyproject.toml"
    ).read_text(encoding="utf-8")

    assert init_text.count(public_doctor) >= 3
    assert init_text.index("ad-migration --version") < init_text.index(public_doctor)
    assert init_text.index("## Step 2: Runtime selection") < init_text.index(
        "7. `ad-migration doctor drivers --project-root . --json`"
    )
    assert '"ad-migration-shared[export,oracle]==0.1.0"' in internal_pyproject
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import pydantic, sqlglot, typer\"" in init_text
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import pyodbc\"" in init_text
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import oracledb\"" in init_text
    assert "Fix the public CLI package or Homebrew formula resources" in init_text
    assert "stop before handing the user to `ad-migration setup-target` or `ad-migration setup-sandbox`" in init_text
    assert "Do not tell the user to run `pip install`, `uv pip install`, or otherwise mutate the brewed virtualenv" in init_text
