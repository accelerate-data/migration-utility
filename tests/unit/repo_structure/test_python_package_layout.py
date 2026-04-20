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
        "ad_migration_replicate_source_tables": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration replicate-source-tables",
        "ad_migration_exclude_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration exclude-table",
        "ad_migration_add_source_table": f"cd {PUBLIC_PROJECT_PATH} && uv run ad-migration add-source-table",
    }

    command_surface_entries = {
        "migrate_mart_plan_command": "/migrate-mart-plan <slug>",
        "migrate_mart_command": "/migrate-mart <plan-file>",
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
    assert command_surface_entries.items() <= repo_map["commands"].items()
    assert "init_discover_mssql_driver_override" not in repo_map["commands"]

    for command_name, expected_prefix in public_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)

    for command_name, expected_prefix in internal_cli_commands.items():
        assert repo_map["commands"][command_name].startswith(expected_prefix)


def test_repo_map_documents_plugin_runtime_scripts() -> None:
    repo_map = json.loads((REPO_ROOT / "repo-map.json").read_text(encoding="utf-8"))

    assert "scripts/" in repo_map["key_directories"]
    scripts_description = repo_map["key_directories"]["scripts/"]
    assert "maintainer development helper" in scripts_description
    assert "deterministic plugin runtime helpers" in scripts_description


def test_maintainer_docs_use_the_internal_project_path() -> None:
    internal_files = [
        "commands/init-ad-migration.md",
        "commands/generate-model.md",
        "commands/generate-tests.md",
        "commands/refactor-query.md",
        "commands/refactor-mart-plan.md",
        "commands/migrate-mart-plan.md",
        "commands/migrate-mart.md",
        "commands/refactor-mart.md",
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
        "skills/planning-refactor-mart/SKILL.md",
        "skills/planning-refactor-mart/references/plan-file-contract.md",
        "skills/applying-staging-candidate/SKILL.md",
        "skills/applying-staging-candidate/references/staging-validation-contract.md",
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


def test_migrate_mart_plan_uses_stage_specific_worktrees_and_handoff() -> None:
    migrate_mart_plan_text = (REPO_ROOT / "commands/migrate-mart-plan.md").read_text(
        encoding="utf-8"
    )

    expected_snippets = [
        "This command opens or updates the planning PR for the generated plan branch, then stops.",
        "It does not execute migration stages and does not open the final coordinator PR.",
        '"${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" "feature/migrate-mart-<slug>" "migrate-mart-<slug>" "<default-branch>"',
        'uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan --project-root <worktree_path>',
        "Do not write angle-bracket placeholders into executable stage metadata.",
        "## Coordinator",
        "## Stage 010: Runtime Readiness",
        "- Status: `complete`",
        "## Stage 020: Scope",
        "- Status: `complete` or `skipped`",
        "## Stage 030: Catalog Ownership Check",
        "- Status: `complete`",
        "- Slash command: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`",
        "- Invocation: `/scope-tables <plan-file> 020 020-scope-<slug> feature/migrate-mart-<slug> <scope-targets>`",
        "- Branch: `feature/migrate-mart-<slug>/040-profile-<slug>`",
        "- Worktree name: `040-profile-<slug>`",
        "- Worktree path: `../worktrees/feature/migrate-mart-<slug>/040-profile-<slug>`",
        "- Branch: `feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`",
        "- Worktree name: `120-refactor-mart-higher-<slug>`",
        "- Worktree path: `../worktrees/feature/migrate-mart-<slug>/120-refactor-mart-higher-<slug>`",
        "`/migrate-mart-plan` writes the plan, opens the planning PR, and stops;",
    ]

    for snippet in expected_snippets:
        assert snippet in migrate_mart_plan_text

    assert "### Coordinator" not in migrate_mart_plan_text
    assert "### Stage 010: Runtime Readiness" not in migrate_mart_plan_text


def test_migrate_mart_command_handles_resume_and_final_pr_behavior() -> None:
    migrate_mart_text = (REPO_ROOT / "commands/migrate-mart.md").read_text(encoding="utf-8")

    expected_snippets = [
        "name: migrate-mart",
        "description: Execute a migrate-mart Markdown plan by resuming the first incomplete task, launching one stage subagent at a time, merging stage PRs, and updating the coordinator plan.",
        'argument-hint: "<plan-file>"',
        "If plan metadata is malformed or missing, mark the coordinator blocked with `PLAN_INVALID` and stop.",
        "Verify pre-execution stages 010, 020, and 030 have stable `complete`, `skipped`, or `superseded` status.",
        "Pick the first executable stage with `Status` not in `complete`, `skipped`, or `superseded`.",
        "existing stage worktree with incomplete work: relaunch recorded invocation",
        "stage branch commits without PR: call `stage-pr.sh`",
        "open PR: call `stage-pr-merge.sh`",
        "already merged PR: mark merge complete",
        "merged stage with remaining worktree: call `stage-cleanup.sh`",
        'After each merge, refresh coordinator worktree, rerun `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan --project-root <worktree_path>`, update the Markdown plan, and commit the plan update.',
        "Launch exactly one subagent at a time.",
        "| 040 | recorded `/profile-tables ...` invocation |",
        "| 050 | deterministic `ad-migration setup-target` stage subagent |",
        "| 060 | deterministic `ad-migration setup-sandbox --yes` stage subagent |",
        "| 070 | recorded `/generate-tests ...` invocation |",
        "| 080 | recorded `/refactor-query ...` invocation |",
        "| 090 | recorded `ad-migration replicate-source-tables --limit <plan-limit> --yes` invocation |",
        "| 100 | recorded `/generate-model ...` invocation |",
        "| 110 | recorded `/refactor-mart ... stg` invocation |",
        "| 120 | recorded `/refactor-mart ... int` invocation |",
        "Scan executable stage sections 040 through 120 in numeric order.",
        "When all stages are complete, open or update the final coordinator PR from the coordinator branch to the remote default branch. Do not merge the final coordinator PR. Report the URL for human review.",
        "When stages 040 through 120 are complete, update Stage 130 and open or update the final coordinator PR.",
    ]

    for snippet in expected_snippets:
        assert snippet in migrate_mart_text


def test_eval_smoke_includes_migrate_mart_command_packages() -> None:
    package_json = json.loads(
        (REPO_ROOT / "tests/evals/package.json").read_text(encoding="utf-8")
    )
    smoke_script = package_json["scripts"]["eval:smoke"]

    expected_snippets = [
        "-c packages/cmd-migrate-mart-plan/cmd-migrate-mart-plan.yaml",
        "-c packages/cmd-migrate-mart/cmd-migrate-mart.yaml",
    ]

    for snippet in expected_snippets:
        assert snippet in smoke_script


def test_repo_map_includes_migrate_mart_eval_commands() -> None:
    repo_map = json.loads((REPO_ROOT / "repo-map.json").read_text(encoding="utf-8"))

    expected_commands = {
        "eval_cmd_migrate_mart_plan": "cd tests/evals && npm run eval:cmd-migrate-mart-plan",
        "eval_cmd_migrate_mart": "cd tests/evals && npm run eval:cmd-migrate-mart",
    }

    for command_name, command_value in expected_commands.items():
        assert repo_map["commands"][command_name] == command_value


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
    assert '"ad-migration-shared[export,oracle,sql-server]==0.1.0"' in internal_pyproject
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import pydantic, sqlglot, typer\"" in init_text
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import pyodbc\"" in init_text
    assert "uv run --project \"${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal\" python3 -c \"import oracledb\"" in init_text
    assert "Fix the public CLI package or Homebrew formula resources" in init_text
    assert "stop before handing the user to `ad-migration setup-target` or `ad-migration setup-sandbox`" in init_text
    assert "Do not tell the user to run `pip install`, `uv pip install`, or otherwise mutate the brewed virtualenv" in init_text


def test_init_command_documents_supported_host_platforms() -> None:
    init_text = (REPO_ROOT / "commands/init-ad-migration.md").read_text(encoding="utf-8")

    assert "macOS" in init_text
    assert "Linux" in init_text
    assert "WSL" in init_text
    assert "Native Windows" in init_text
    assert "Use WSL" in init_text
    assert "brew install freetds" in init_text
    assert "platform package manager" in init_text
    assert "Do not attempt `brew install` on Linux or WSL." in init_text


def test_refactor_mart_staging_execution_artifacts_exist() -> None:
    expected_paths = [
        "skills/applying-staging-candidate/SKILL.md",
        "skills/applying-staging-candidate/references/staging-validation-contract.md",
        "tests/evals/prompts/skill-applying-staging-candidate.txt",
        "tests/evals/prompts/cmd-refactor-mart-stg.txt",
        "tests/evals/packages/applying-staging-candidate/skill-applying-staging-candidate.yaml",
        "tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml",
    ]

    for relative_path in expected_paths:
        assert (REPO_ROOT / relative_path).exists(), relative_path

    package_json = json.loads(
        (REPO_ROOT / "tests/evals/package.json").read_text(encoding="utf-8")
    )
    scripts = package_json["scripts"]
    assert "eval:applying-staging-candidate" in scripts
    assert "eval:cmd-refactor-mart" in scripts


def test_refactor_mart_eval_packages_are_in_smoke() -> None:
    package_json = json.loads(
        (REPO_ROOT / "tests/evals/package.json").read_text(encoding="utf-8")
    )
    smoke_script = package_json["scripts"]["eval:smoke"]

    expected_packages = [
        "packages/planning-refactor-mart/skill-planning-refactor-mart.yaml",
        "packages/applying-staging-candidate/skill-applying-staging-candidate.yaml",
        "packages/cmd-refactor-mart/cmd-refactor-mart.yaml",
    ]

    for package_path in expected_packages:
        assert package_path in smoke_script, package_path


def test_status_command_examples_pass_project_root() -> None:
    status_text = (REPO_ROOT / "commands/status.md").read_text(encoding="utf-8")

    required_examples = [
        "migrate-util sync-excluded-warnings --project-root <project-root>",
        "migrate-util batch-plan --project-root <project-root>",
        "migrate-util status <schema.table> --project-root <project-root>",
        "migrate-util ready test-gen --project-root <project-root> --object <schema.table>",
    ]

    for example in required_examples:
        assert example in status_text
