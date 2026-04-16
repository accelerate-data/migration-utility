import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

import shared.cli.error_handler as _err_mod
from shared.cli.main import app
from shared.cli.setup_sandbox_cmd import _write_sandbox_connection_to_manifest
from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
)

runner = CliRunner()


def _write_manifest(tmp_path: Path, with_sandbox: bool = False) -> None:
    manifest = {
        "schema_version": "1",
        "technology": "sql_server",
        "runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {}}},
        "extraction": {"schemas": ["silver"]},
    }
    if with_sandbox:
        manifest["runtime"]["sandbox"] = {"technology": "sql_server", "dialect": "tsql", "connection": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


_SANDBOX_UP_OUT = SandboxUpOutput(
    sandbox_database="__test_abc123",
    status="ok",
    tables_cloned=["silver.DimCustomer"],
    views_cloned=[],
    procedures_cloned=["silver.usp_load"],
    errors=[],
)
_SANDBOX_DOWN_OUT = SandboxDownOutput(sandbox_database="__test_abc123", status="ok")


def test_setup_sandbox_runs_sandbox_up(tmp_path):
    _write_manifest(tmp_path)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_up.assert_called_once()
    assert "Review and commit the repo changes before continuing" in result.output


def test_setup_sandbox_reuses_existing_canonical_sandbox(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="__test_existing",
        status="ok",
        exists=True,
        has_content=True,
        tables_count=1,
        views_count=0,
        procedures_count=1,
    )

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_status.assert_called_once_with("__test_existing", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "__test_existing")
    assert "already exists" in result.output


def test_setup_sandbox_repairs_existing_empty_canonical_sandbox(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="__test_existing",
        status="ok",
        exists=True,
        has_content=False,
        tables_count=0,
        views_count=0,
        procedures_count=0,
    )
    mock_backend.sandbox_reset.return_value = SandboxUpOutput(
        sandbox_database="__test_existing",
        status="ok",
        tables_cloned=["silver.DimCustomer"],
        views_cloned=[],
        procedures_cloned=["silver.usp_load"],
        errors=[],
    )

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_status.assert_called_once_with("__test_existing", schemas=["silver"])
    mock_backend.sandbox_reset.assert_called_once_with("__test_existing", schemas=["silver"])
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "__test_existing")
    assert "repairing" in result.output


def test_setup_sandbox_reuses_existing_oracle_sandbox_schema(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"schema_name": "__test_existing"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="__test_existing",
        status="ok",
        exists=True,
        has_content=True,
        tables_count=1,
        views_count=0,
        procedures_count=1,
    )

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="oracle"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["SH"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_status.assert_called_once_with("__test_existing", schemas=["SH"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "__test_existing")


def test_setup_sandbox_surfaces_status_errors_without_provisioning(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        }
    }
    manifest_path = tmp_path / "manifest.json"
    before = manifest_path.read_bytes()
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="__test_existing",
        status="error",
        exists=False,
        errors=[
            ErrorEntry(
                code="SANDBOX_STATUS_FAILED",
                message="connection failed",
            )
        ],
    )

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1, result.output
    assert "SANDBOX_STATUS_FAILED" in result.output
    mock_backend.sandbox_status.assert_called_once_with("__test_existing", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_not_called()
    assert manifest_path.read_bytes() == before


def test_setup_sandbox_creates_new_when_canonical_sandbox_not_found(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_missing"},
            }
        },
        "sandbox": {"database": "__test_legacy_ignored"},
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="__test_missing",
        status="not_found",
        exists=False,
    )
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_status.assert_called_once_with("__test_missing", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_called_once_with(schemas=["silver"])
    mock_write_sandbox.assert_called_once_with(tmp_path, "__test_abc123")


def test_write_sandbox_connection_preserves_existing_canonical_name_and_ignores_legacy(tmp_path, monkeypatch):
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        },
        "sandbox": {"database": "__test_legacy_ignored"},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "127.0.0.1")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sa")
    monkeypatch.setenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    updated = _write_sandbox_connection_to_manifest(tmp_path, manifest, "sql_server")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["database"] == "__test_existing"
    assert connection["host"] == "127.0.0.1"
    assert updated["sandbox"]["database"] == "__test_legacy_ignored"


def test_teardown_sandbox_requires_confirmation(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={"runtime": {"sandbox": {}}}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        # User enters 'n' at the prompt
        result = runner.invoke(app, ["teardown-sandbox", "--project-root", str(tmp_path)], input="n\n")

    assert result.exit_code == 0
    assert "Review and commit the repo changes before continuing" not in result.output


def test_teardown_sandbox_yes_flag_skips_prompt(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0
    mock_backend.sandbox_down.assert_called_once_with("__test_abc123")
    assert "Review and commit the repo changes before continuing" in result.output


def test_teardown_sandbox_no_sandbox_exits_1(tmp_path):
    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value=None),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "Review and commit the repo changes before continuing" not in result.output


def test_teardown_sandbox_error_exits_nonzero(tmp_path):
    from shared.output_models.sandbox import SandboxDownOutput
    error_out = SandboxDownOutput(sandbox_database="__test_abc123", status="error")
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = error_out

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1


def _patch_pyodbc_programming():
    class _FakePyodbcProgramming(Exception): pass
    return _FakePyodbcProgramming, patch.multiple(
        _err_mod,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_INTERFACE_ERROR=None,
        _PYODBC_OPERATIONAL_ERROR=None,
        _PYODBC_ERROR=_FakePyodbcProgramming,
    )


def test_setup_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_up.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output
    assert "Review and commit the repo changes before continuing" not in result.output


def test_setup_sandbox_mentions_restart_when_source_env_exists_in_dotenv(tmp_path):
    manifest = {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "password_env": "SOURCE_MSSQL_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "password_env": "SANDBOX_MSSQL_PASSWORD",
                },
            },
        }
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (tmp_path / ".env").write_text("SOURCE_MSSQL_PASSWORD=source-pass\n", encoding="utf-8")
    backend_cls = MagicMock()
    backend_cls.from_env.side_effect = ValueError(
        "Required sandbox configuration is missing: "
        "['environment variable referenced by runtime.source.connection.password_env (SOURCE_MSSQL_PASSWORD)']"
    )

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
        patch("shared.test_harness_support.manifest.get_backend", return_value=backend_cls),
        patch.dict(os.environ, {}, clear=True),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "SOURCE_MSSQL_PASSWORD" in result.output
    assert "defined in .env" in result.output
    assert "direnv" in result.output


def test_setup_sandbox_mentions_restart_when_sandbox_env_check_finds_dotenv_value(tmp_path):
    _write_manifest(tmp_path)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {},
            }
        }
    }
    (tmp_path / ".env").write_text("SANDBOX_MSSQL_PASSWORD=sandbox-pass\n", encoding="utf-8")

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch.dict(
            os.environ,
            {
                "SANDBOX_MSSQL_HOST": "localhost",
                "SANDBOX_MSSQL_PORT": "1433",
                "SANDBOX_MSSQL_USER": "sa",
            },
            clear=True,
        ),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
    assert "SANDBOX_MSSQL_PASSWORD" in result.output
    assert "defined in .env" in result.output
    assert "direnv" in result.output


def test_setup_sandbox_calls_require_sandbox_vars(tmp_path):
    """setup-sandbox must call require_sandbox_vars with the sandbox technology."""
    _write_manifest(tmp_path)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars") as mock_require,
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_require.assert_called_once_with("sql_server", tmp_path)


def test_setup_sandbox_writes_connection_to_manifest(tmp_path):
    """setup-sandbox writes refreshed connection metadata after successful provisioning."""
    _write_manifest(tmp_path)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}) as mock_write,
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once()


def test_setup_sandbox_exits_1_when_sandbox_role_missing(tmp_path):
    """setup-sandbox exits 1 if manifest has no runtime.sandbox technology."""
    _write_manifest(tmp_path)

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", side_effect=SystemExit(1)),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1


def test_teardown_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output
