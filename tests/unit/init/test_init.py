"""Tests for init.py CLI.

Unit tests verify scaffold-project and scaffold-hooks produce correct output.
Tests call run_* functions directly (testability pattern).
"""

from __future__ import annotations

import json
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import init_templates
from shared.init import (
    app,
    classify_host_platform,
    GITIGNORE_ENTRIES,
    SOURCE_REGISTRY,
    get_source_config,
    HostPlatform,
    build_init_platform_gate_message,
    run_scaffold_hooks,
    run_scaffold_project,
    supports_homebrew_install,
    supports_native_windows,
    write_local_env_overrides,
)

RUNNER = CliRunner()

SCAFFOLD_GOLDEN_HASHES = {
    "sql_server": {
        "CLAUDE.md": "54388313acccc4f0b06c588010b3a336a8746faad78299fb874fb787df366f8d",
        "README.md": "33bf498002cb54876a2a544109c7ef6b98664673b73c62b99c3322445b6f4824",
        ".envrc": "05377c6d9606aea1451ca4b96d351252fdfe26e6d4ddc815e000af12e3c9f8ac",
        "repo-map.json": "e706817c0eb802839cc8d26d474e604ec63cd350cc0ba8f2f043e6e34ed098ef",
    },
    "oracle": {
        "CLAUDE.md": "881fcfa23518c3089059bd459e73ca2e6c3fc4efb833eb12ccefe04c385ee8b1",
        "README.md": "76965ef2d1d770a804ee27e69b493003785fd9ecea26cf94e80fd4d4655da347",
        ".envrc": "8f730cba08e545abe16e01b66b8a3a18ae0e20fa15e2e1fae3fb3d3f86c7710d",
        "repo-map.json": "b4fc3898be790dfeffb7ecd90666e61e94139672e2b70a8caf609334ff17f619",
    },
}
HOOK_GOLDEN_HASH = "647af779051fd9abb37885d0b46c9baf07a56fa2f4bd3e4afbd72ab28dfee025"


class TestHostPlatform:
    def test_classifies_macos_as_supported_homebrew_platform(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("shared.platform.platform.system", lambda: "Darwin")

        result = classify_host_platform()

        assert result == HostPlatform(slug="macos", supported=True, display_name="macOS")
        assert supports_homebrew_install(result) is True
        assert supports_native_windows(result) is False

    def test_classifies_linux_as_supported_non_homebrew_platform(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("shared.platform.platform.system", lambda: "Linux")
        monkeypatch.setattr("shared.platform._read_osrelease_text", lambda: "NAME=Ubuntu\n")
        monkeypatch.setattr("shared.platform._read_proc_version_text", lambda: "Linux version 6.8")

        result = classify_host_platform()

        assert result == HostPlatform(slug="linux", supported=True, display_name="Linux")
        assert supports_homebrew_install(result) is False

    def test_classifies_wsl_as_supported_linux_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("shared.platform.platform.system", lambda: "Linux")
        monkeypatch.setattr("shared.platform._read_osrelease_text", lambda: "NAME=Ubuntu\n")
        monkeypatch.setattr(
            "shared.platform._read_proc_version_text",
            lambda: "Linux version 6.6.87.2-microsoft-standard-WSL2",
        )

        result = classify_host_platform()

        assert result == HostPlatform(slug="wsl", supported=True, display_name="WSL")
        assert supports_homebrew_install(result) is False

    def test_classifies_native_windows_as_unsupported(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("shared.platform.platform.system", lambda: "Windows")

        result = classify_host_platform()

        assert result == HostPlatform(slug="windows", supported=False, display_name="Windows")
        assert supports_native_windows(result) is True
        assert "Use WSL" in build_init_platform_gate_message(result)


# ── scaffold-project (sql_server default) ───────────────────────────────────


class TestScaffoldProject:
    def test_creates_all_files_in_empty_dir(self, tmp_path: Path) -> None:
        config = get_source_config("sql_server")
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result.files_created
        assert "README.md" in result.files_created
        assert "repo-map.json" in result.files_created
        assert ".gitignore" in result.files_created
        assert ".envrc" in result.files_created
        assert ".claude/rules/git-workflow.md" in result.files_created
        assert result.files_updated == []
        assert result.files_skipped == []
        assert sorted(result.written_paths) == sorted(result.files_created)

        # Verify file contents match sql_server templates
        assert (tmp_path / "CLAUDE.md").read_text() == config.claude_md_fn()
        assert (tmp_path / "README.md").read_text() == config.readme_md_fn()
        repo_map = json.loads((tmp_path / "repo-map.json").read_text())
        assert repo_map == config.repo_map_fn()
        assert ".mcp.json" not in (tmp_path / ".gitignore").read_text()
        assert ".envrc" not in (tmp_path / ".gitignore").read_text()
        envrc_text = (tmp_path / ".envrc").read_text()
        assert "MSSQL_HOST" in envrc_text
        assert "SOURCE_MSSQL_PASSWORD" not in envrc_text
        assert "SANDBOX_MSSQL_PASSWORD" not in envrc_text
        assert "TARGET_MSSQL_PASSWORD" not in envrc_text
        assert 'source_env_if_exists .env' in (tmp_path / ".envrc").read_text()
        workflow = (tmp_path / ".claude" / "rules" / "git-workflow.md").read_text()
        assert "Worktree" in workflow
        assert "../worktrees" in workflow
        assert "git-checkpoints" in workflow
        assert "scripts/worktree.sh" not in workflow
        assert not (tmp_path / "scripts" / "worktree.sh").exists()

    def test_idempotent_skips_existing_files(self, tmp_path: Path) -> None:
        run_scaffold_project(tmp_path)
        result = run_scaffold_project(tmp_path)
        assert result.files_created == []
        assert result.files_updated == []
        assert len(result.files_skipped) == 6
        assert result.written_paths == []

    def test_merges_missing_gitignore_entries(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("# Custom\n.DS_Store\n")
        result = run_scaffold_project(tmp_path)
        updated = [f for f in result.files_updated if f.startswith(".gitignore")]
        assert len(updated) == 1
        assert ".gitignore" in result.written_paths
        content = (tmp_path / ".gitignore").read_text()
        assert "# Custom" in content
        assert ".mcp.json" not in content
        assert ".envrc" not in content

    def test_merges_local_env_loader_into_existing_envrc(self, tmp_path: Path) -> None:
        (tmp_path / ".envrc").write_text("export MSSQL_HOST=localhost\n", encoding="utf-8")

        result = run_scaffold_project(tmp_path)

        assert ".envrc (+local .env loader)" in result.files_updated
        assert ".envrc" in result.written_paths
        envrc = (tmp_path / ".envrc").read_text()
        assert "export MSSQL_HOST=localhost" in envrc
        assert "source_env_if_exists .env" in envrc

    def test_reports_missing_claude_md_sections(self, tmp_path: Path) -> None:
        (tmp_path / "CLAUDE.md").write_text("# Project\n\n## Domain\n\nSome domain info.\n")
        result = run_scaffold_project(tmp_path)
        skipped = [f for f in result.files_skipped if f.startswith("CLAUDE.md")]
        assert len(skipped) == 1
        assert "missing sections" in skipped[0]

    def test_complete_claude_md_skipped_cleanly(self, tmp_path: Path) -> None:
        config = get_source_config("sql_server")
        (tmp_path / "CLAUDE.md").write_text(config.claude_md_fn())
        result = run_scaffold_project(tmp_path)
        assert "CLAUDE.md" in result.files_skipped


# ── scaffold-project (oracle) ───────────────────────────────────────────────


class TestScaffoldProjectOracle:
    def test_creates_oracle_files(self, tmp_path: Path) -> None:
        result = run_scaffold_project(tmp_path, technology="oracle")
        assert "CLAUDE.md" in result.files_created
        assert ".envrc" in result.files_created
        assert "CLAUDE.md" in result.written_paths
        assert ".envrc" in result.written_paths

        # Oracle-specific content
        envrc = (tmp_path / ".envrc").read_text()
        assert "ORACLE_HOST" in envrc
        assert "ORACLE_PORT" in envrc
        assert "ORACLE_SERVICE" in envrc
        assert "ORACLE_USER" in envrc
        assert "ORACLE_PASSWORD" not in envrc
        assert 'source_env_if_exists .env' in envrc
        assert "MSSQL" not in envrc

        claude_md = (tmp_path / "CLAUDE.md").read_text()
        assert "Oracle" in claude_md
        assert "ddl_mcp" in claude_md

        readme = (tmp_path / "README.md").read_text()
        assert "Oracle" in readme

        repo_map = json.loads((tmp_path / "repo-map.json").read_text())
        assert "oracle_env_vars" in repo_map["notes_for_agents"]
        assert "mssql_env_vars" not in repo_map["notes_for_agents"]

    def test_invalid_technology_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unknown technology"):
            run_scaffold_project(tmp_path, technology="postgres")


# ── scaffold-hooks ───────────────────────────────────────────────────────────


class TestScaffoldHooks:
    def test_creates_pre_commit_hook(self, tmp_path: Path) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        result = run_scaffold_hooks(tmp_path)
        assert result.hook_created is True
        assert result.written_paths == [".githooks/pre-commit"]
        assert result.hooks_path_configured is True

        hook_path = tmp_path / ".githooks" / "pre-commit"
        assert hook_path.exists()
        assert hook_path.stat().st_mode & 0o111  # executable
        assert "ANT_KEY_PAT" in hook_path.read_text()

    def test_idempotent_skips_existing_hook(self, tmp_path: Path) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        run_scaffold_hooks(tmp_path)
        result = run_scaffold_hooks(tmp_path)
        assert result.hook_created is False
        assert result.written_paths == []
        assert result.hooks_path_configured is True

    def test_no_git_repo_still_creates_hook(self, tmp_path: Path) -> None:
        result = run_scaffold_hooks(tmp_path)
        assert result.hook_created is True
        assert result.hooks_path_configured is False
        assert (tmp_path / ".githooks" / "pre-commit").exists()

    def test_oracle_hook_blocks_oracle_creds(self, tmp_path: Path) -> None:
        run_scaffold_hooks(tmp_path, technology="oracle")
        hook_content = (tmp_path / ".githooks" / "pre-commit").read_text()
        assert "tracked secret field" in hook_content
        assert "API_KEY" in hook_content
        assert "MSSQL" not in hook_content

    def test_sql_server_hook_allows_tracked_non_secret_envrc(self, tmp_path: Path) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        run_scaffold_hooks(tmp_path)
        envrc_path = tmp_path / ".envrc"
        envrc_path.write_text(
            'export SOURCE_MSSQL_HOST=localhost\n'
            'export SOURCE_MSSQL_USER=sa\n',
            encoding="utf-8",
        )
        subprocess.run(["git", "add", ".envrc"], cwd=tmp_path, capture_output=True, check=True)

        result = subprocess.run(
            [str(tmp_path / ".githooks" / "pre-commit")],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr

    @pytest.mark.parametrize(
        ("filename", "content"),
        [
            (".envrc", "export SOURCE_MSSQL_PASSWORD=super-secret\n"),
            (".mcp.json", '{"api_key":"secret-value"}\n'),
        ],
    )
    def test_sql_server_hook_blocks_tracked_secrets(
        self,
        tmp_path: Path,
        filename: str,
        content: str,
    ) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        run_scaffold_hooks(tmp_path)
        target = tmp_path / filename
        target.write_text(content, encoding="utf-8")
        subprocess.run(["git", "add", filename], cwd=tmp_path, capture_output=True, check=True)

        result = subprocess.run(
            [str(tmp_path / ".githooks" / "pre-commit")],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 1

    @pytest.mark.parametrize("technology", ["sql_server", "oracle"])
    def test_pre_commit_hook_output_matches_golden_hash(
        self,
        tmp_path: Path,
        technology: str,
    ) -> None:
        subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
        run_scaffold_hooks(tmp_path, technology=technology)

        hook_content = (tmp_path / ".githooks" / "pre-commit").read_text(encoding="utf-8")

        assert sha256(hook_content.encode()).hexdigest() == HOOK_GOLDEN_HASH


# ── source registry ─────────────────────────────────────────────────────────


class TestSourceRegistry:
    def test_registry_has_both_sources(self) -> None:
        assert "sql_server" in SOURCE_REGISTRY
        assert "oracle" in SOURCE_REGISTRY

    def test_get_source_config_returns_config(self) -> None:
        config = get_source_config("sql_server")
        assert config.slug == "sql_server"
        assert config.display_name == "SQL Server"

    def test_get_source_config_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown technology"):
            get_source_config("postgres")

    def test_each_source_has_callable_templates(self) -> None:
        for slug, config in SOURCE_REGISTRY.items():
            assert callable(config.claude_md_fn), f"{slug} claude_md_fn not callable"
            assert callable(config.readme_md_fn), f"{slug} readme_md_fn not callable"
            assert callable(config.envrc_fn), f"{slug} envrc_fn not callable"
            assert callable(config.repo_map_fn), f"{slug} repo_map_fn not callable"
            assert callable(config.pre_commit_hook_fn), f"{slug} pre_commit_hook_fn not callable"
            # Verify they return non-empty content
            assert len(config.claude_md_fn()) > 0
            assert len(config.readme_md_fn()) > 0
            assert len(config.envrc_fn()) > 0
            assert len(config.repo_map_fn()) > 0
            assert len(config.pre_commit_hook_fn()) > 0


class TestSourceTemplateConfig:
    def test_source_template_configs_hold_explicit_technology_values(self) -> None:
        configs = init_templates.SOURCE_TEMPLATE_CONFIGS

        assert set(configs) == {"sql_server", "oracle"}
        assert configs["sql_server"].source_system == "Microsoft SQL Server"
        assert configs["sql_server"].procedure_language == "T-SQL"
        assert configs["sql_server"].env_prefix == "MSSQL"
        assert configs["sql_server"].example_database == "YourDatabase"
        assert configs["oracle"].source_system == "Oracle Database"
        assert configs["oracle"].procedure_language == "PL/SQL"
        assert configs["oracle"].env_prefix == "ORACLE"
        assert configs["oracle"].example_service == "FREEPDB1"

    @pytest.mark.parametrize("technology", ["sql_server", "oracle"])
    def test_scaffold_project_output_matches_golden_hashes(
        self,
        tmp_path: Path,
        technology: str,
    ) -> None:
        run_scaffold_project(tmp_path, technology=technology)

        for relative_path, expected_hash in SCAFFOLD_GOLDEN_HASHES[technology].items():
            content = (tmp_path / relative_path).read_text(encoding="utf-8")
            assert sha256(content.encode()).hexdigest() == expected_hash


class TestWriteLocalEnvOverrides:
    def test_writes_new_local_env_file(self, tmp_path: Path) -> None:
        changed = write_local_env_overrides(
            tmp_path,
            {
                "LOCAL_TOOL_PATH": "/opt/tool/bin",
            },
        )

        assert changed is True
        assert (tmp_path / ".env").read_text() == 'LOCAL_TOOL_PATH="/opt/tool/bin"\n'

    def test_updates_existing_keys_without_touching_other_lines(self, tmp_path: Path) -> None:
        env_path = tmp_path / ".env"
        env_path.write_text(
            'KEEP_ME="1"\n'
            'LOCAL_TOOL_PATH="/old/path"\n',
            encoding="utf-8",
        )

        changed = write_local_env_overrides(
            tmp_path,
            {"LOCAL_TOOL_PATH": "/opt/tool/bin"},
        )

        assert changed is True
        assert env_path.read_text() == (
            'KEEP_ME="1"\n'
            'LOCAL_TOOL_PATH="/opt/tool/bin"\n'
        )

    def test_returns_false_when_no_changes_are_needed(self, tmp_path: Path) -> None:
        env_path = tmp_path / ".env"
        env_path.write_text('LOCAL_TOOL_PATH="/opt/tool/bin"\n', encoding="utf-8")

        changed = write_local_env_overrides(
            tmp_path,
            {"LOCAL_TOOL_PATH": "/opt/tool/bin"},
        )

        assert changed is False
        assert env_path.read_text() == 'LOCAL_TOOL_PATH="/opt/tool/bin"\n'

    def test_escapes_quotes_and_backslashes(self, tmp_path: Path) -> None:
        changed = write_local_env_overrides(
            tmp_path,
            {"LOCAL_TOOL_PATH": 'C:\\Program Files\\"tool"'},
        )

        assert changed is True
        assert (tmp_path / ".env").read_text() == (
            'LOCAL_TOOL_PATH="C:\\\\Program Files\\\\\\"tool\\""\n'
        )

    def test_cli_writes_local_env_overrides(self, tmp_path: Path) -> None:
        result = RUNNER.invoke(
            app,
            [
                "write-local-env-overrides",
                "--project-root", str(tmp_path),
                "--overrides-json", '{"LOCAL_TOOL_PATH":"/opt/tool/bin"}',
            ],
        )

        assert result.exit_code == 0, result.stdout
        payload = json.loads(result.stdout)
        assert payload["changed"] is True
        assert payload["file"].endswith("/.env")

    def test_cli_rejects_invalid_json(self, tmp_path: Path) -> None:
        result = RUNNER.invoke(
            app,
            [
                "write-local-env-overrides",
                "--project-root", str(tmp_path),
                "--overrides-json", "{not-json}",
            ],
        )

        assert result.exit_code == 1
        assert "Invalid --overrides-json" in result.stderr

    def test_cli_rejects_non_string_map(self, tmp_path: Path) -> None:
        result = RUNNER.invoke(
            app,
            [
                "write-local-env-overrides",
                "--project-root", str(tmp_path),
                "--overrides-json", '{"LOCAL_TOOL_PATH":123}',
            ],
        )

        assert result.exit_code == 1
        assert "must be a JSON object of string keys and string values" in result.stderr
