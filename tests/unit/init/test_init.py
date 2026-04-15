"""Tests for init.py CLI.

Unit tests verify scaffold-project and scaffold-hooks produce correct output.
Tests call run_* functions directly (testability pattern).
"""

from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared.init import (
    app,
    GITIGNORE_ENTRIES,
    SOURCE_REGISTRY,
    get_source_config,
    run_discover_mssql_driver_override,
    run_scaffold_hooks,
    run_scaffold_project,
    write_local_env_overrides,
)
from shared.output_models.init import LocalOverrideDiscoveryOutput

RUNNER = CliRunner()


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
        assert "scripts/worktree.sh" in result.files_created
        assert result.files_updated == []
        assert result.files_skipped == []

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
        worktree_script = tmp_path / "scripts" / "worktree.sh"
        assert worktree_script.exists()
        assert worktree_script.stat().st_mode & stat.S_IXUSR
        worktree_script_text = worktree_script.read_text()
        assert "WORKTREE_BRANCH_ALREADY_CHECKED_OUT" in worktree_script_text
        assert 'local lib_dir="$worktree_path/lib"' in worktree_script_text
        assert 'local lib_dir="$worktree_path/plugin' not in worktree_script_text

    def test_idempotent_skips_existing_files(self, tmp_path: Path) -> None:
        run_scaffold_project(tmp_path)
        result = run_scaffold_project(tmp_path)
        assert result.files_created == []
        assert result.files_updated == []
        assert len(result.files_skipped) == 7

    def test_merges_missing_gitignore_entries(self, tmp_path: Path) -> None:
        (tmp_path / ".gitignore").write_text("# Custom\n.DS_Store\n")
        result = run_scaffold_project(tmp_path)
        updated = [f for f in result.files_updated if f.startswith(".gitignore")]
        assert len(updated) == 1
        content = (tmp_path / ".gitignore").read_text()
        assert "# Custom" in content
        assert ".mcp.json" not in content
        assert ".envrc" not in content

    def test_merges_local_env_loader_into_existing_envrc(self, tmp_path: Path) -> None:
        (tmp_path / ".envrc").write_text("export MSSQL_HOST=localhost\n", encoding="utf-8")

        result = run_scaffold_project(tmp_path)

        assert ".envrc (+local .env loader)" in result.files_updated
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


class TestWriteLocalEnvOverrides:
    def test_writes_new_local_env_file(self, tmp_path: Path) -> None:
        changed = write_local_env_overrides(
            tmp_path,
            {
                "MSSQL_DRIVER": "ODBC Driver 18 for SQL Server",
                "SQLCL_BIN": "/opt/sqlcl/bin/sql",
            },
        )

        assert changed is True
        assert (tmp_path / ".env").read_text() == (
            'MSSQL_DRIVER="ODBC Driver 18 for SQL Server"\n'
            'SQLCL_BIN="/opt/sqlcl/bin/sql"\n'
        )

    def test_updates_existing_keys_without_touching_other_lines(self, tmp_path: Path) -> None:
        env_path = tmp_path / ".env"
        env_path.write_text(
            'KEEP_ME="1"\n'
            'MSSQL_DRIVER="FreeTDS"\n',
            encoding="utf-8",
        )

        changed = write_local_env_overrides(
            tmp_path,
            {"MSSQL_DRIVER": "ODBC Driver 18 for SQL Server"},
        )

        assert changed is True
        assert env_path.read_text() == (
            'KEEP_ME="1"\n'
            'MSSQL_DRIVER="ODBC Driver 18 for SQL Server"\n'
        )

    def test_returns_false_when_no_changes_are_needed(self, tmp_path: Path) -> None:
        env_path = tmp_path / ".env"
        env_path.write_text('SQLCL_BIN="/opt/sqlcl/bin/sql"\n', encoding="utf-8")

        changed = write_local_env_overrides(
            tmp_path,
            {"SQLCL_BIN": "/opt/sqlcl/bin/sql"},
        )

        assert changed is False
        assert env_path.read_text() == 'SQLCL_BIN="/opt/sqlcl/bin/sql"\n'

    def test_escapes_quotes_and_backslashes(self, tmp_path: Path) -> None:
        changed = write_local_env_overrides(
            tmp_path,
            {"SQLCL_BIN": 'C:\\Program Files\\SQLcl\\"sql".exe'},
        )

        assert changed is True
        assert (tmp_path / ".env").read_text() == (
            'SQLCL_BIN="C:\\\\Program Files\\\\SQLcl\\\\\\"sql\\".exe"\n'
        )

    def test_cli_writes_local_env_overrides(self, tmp_path: Path) -> None:
        result = RUNNER.invoke(
            app,
            [
                "write-local-env-overrides",
                "--project-root", str(tmp_path),
                "--overrides-json", '{"SQLCL_BIN":"/opt/sqlcl/bin/sql"}',
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
                "--overrides-json", '{"SQLCL_BIN":123}',
            ],
        )

        assert result.exit_code == 1
        assert "must be a JSON object of string keys and string values" in result.stderr


class TestDiscoverMssqlDriverOverride:
    def test_uses_non_default_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")
        monkeypatch.setattr(
            "shared.init._query_odbc_drivers",
            lambda: ["ODBC Driver 18 for SQL Server"],
        )

        result = run_discover_mssql_driver_override()

        assert result.status == "resolved"
        assert result.value == "ODBC Driver 18 for SQL Server"

    def test_rejects_unknown_env_driver_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MSSQL_DRIVER", "bogus")
        monkeypatch.setattr("shared.init._query_odbc_drivers", lambda: ["FreeTDS"])

        result = run_discover_mssql_driver_override()

        assert result.status == "manual"
        assert result.message == (
            'Set MSSQL_DRIVER="ODBC Driver 18 for SQL Server" after installing a SQL Server ODBC driver.'
        )

    def test_rejects_default_env_driver_when_freetds_not_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("MSSQL_DRIVER", "FreeTDS")
        monkeypatch.setattr("shared.init._query_odbc_drivers", lambda: ["SQLite3"])

        result = run_discover_mssql_driver_override()

        assert result.status == "manual"

    def test_rejects_default_env_driver_when_different_driver_is_installed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("MSSQL_DRIVER", "FreeTDS")
        monkeypatch.setattr(
            "shared.init._query_odbc_drivers",
            lambda: ["ODBC Driver 18 for SQL Server"],
        )

        result = run_discover_mssql_driver_override()

        assert result.status == "manual"

    def test_defaults_when_freetds_registered(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("MSSQL_DRIVER", raising=False)
        monkeypatch.setattr("shared.init._query_odbc_drivers", lambda: ["FreeTDS"])

        result = run_discover_mssql_driver_override()

        assert result.status == "default"
        assert result.value is None

    def test_resolves_microsoft_driver_from_odbcinst(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("MSSQL_DRIVER", raising=False)
        monkeypatch.setattr(
            "shared.init._query_odbc_drivers",
            lambda: ["ODBC Driver 18 for SQL Server"],
        )

        result = run_discover_mssql_driver_override()

        assert result.status == "resolved"
        assert result.value == "ODBC Driver 18 for SQL Server"

    def test_reports_manual_override_when_no_supported_driver_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("MSSQL_DRIVER", raising=False)
        monkeypatch.setattr("shared.init._query_odbc_drivers", lambda: ["SQLite3"])

        result = run_discover_mssql_driver_override()

        assert result.status == "manual"
        assert result.message == (
            'Set MSSQL_DRIVER="ODBC Driver 18 for SQL Server" after installing a SQL Server ODBC driver.'
        )

    def test_cli_returns_discovery_payload(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "shared.init.run_discover_mssql_driver_override",
            lambda: LocalOverrideDiscoveryOutput(
                key="MSSQL_DRIVER",
                status="resolved",
                value="ODBC Driver 18 for SQL Server",
            ),
        )

        result = RUNNER.invoke(app, ["discover-mssql-driver-override"])

        assert result.exit_code == 0, result.stdout
        payload = json.loads(result.stdout)
        assert payload["key"] == "MSSQL_DRIVER"
        assert payload["status"] == "resolved"
