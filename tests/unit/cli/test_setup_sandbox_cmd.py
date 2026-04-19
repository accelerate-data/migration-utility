import json
import os
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.cli.setup_sandbox_cmd import (
    _build_sandbox_connection_manifest,
    _write_sandbox_connection_to_manifest,
)
from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxStatusOutput,
    SandboxUpOutput,
)
from tests.unit.cli.helpers import _patch_pyodbc_programming, _write_sandbox_manifest

runner = CliRunner()


_SANDBOX_UP_OUT = SandboxUpOutput(
    sandbox_database="SBX_ABC123000000",
    status="ok",
    tables_cloned=["silver.DimCustomer"],
    views_cloned=[],
    procedures_cloned=["silver.usp_load"],
    errors=[],
)

def test_setup_sandbox_runs_sandbox_up(tmp_path):
    _write_sandbox_manifest(tmp_path)
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
    assert "Updated repo state" in result.output
    assert "manifest.json" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_setup_sandbox_confirmation_decline_exits_before_provisioning(tmp_path):
    _write_sandbox_manifest(tmp_path)
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

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value=manifest),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend) as mock_create_backend,
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest") as mock_write_connection,
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest") as mock_write_sandbox,
    ):
        result = runner.invoke(
            app,
            ["setup-sandbox", "--project-root", str(tmp_path)],
            input="n\n",
        )

    assert result.exit_code == 0, result.output
    assert "Aborted." in result.output
    mock_create_backend.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_connection.assert_not_called()
    mock_write_sandbox.assert_not_called()

def test_setup_sandbox_reuses_existing_canonical_sandbox(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SBX_000000000001"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="SBX_000000000001",
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
    mock_backend.sandbox_status.assert_called_once_with("SBX_000000000001", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "SBX_000000000001")
    assert "already exists" in result.output

def test_setup_sandbox_repairs_existing_empty_canonical_sandbox(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SBX_000000000001"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="SBX_000000000001",
        status="ok",
        exists=True,
        has_content=False,
        tables_count=0,
        views_count=0,
        procedures_count=0,
    )
    mock_backend.sandbox_reset.return_value = SandboxUpOutput(
        sandbox_database="SBX_000000000001",
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
    mock_backend.sandbox_status.assert_called_once_with("SBX_000000000001", schemas=["silver"])
    mock_backend.sandbox_reset.assert_called_once_with("SBX_000000000001", schemas=["silver"])
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "SBX_000000000001")
    assert "repairing" in result.output

def test_setup_sandbox_reuses_existing_oracle_sandbox_schema(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"schema_name": "SBX_000000000001"},
            }
        }
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="SBX_000000000001",
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
    mock_backend.sandbox_status.assert_called_once_with("SBX_000000000001", schemas=["SH"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_called_once_with(tmp_path, "SBX_000000000001")

def test_setup_sandbox_surfaces_status_errors_without_provisioning(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SBX_000000000001"},
            }
        }
    }
    manifest_path = tmp_path / "manifest.json"
    before = manifest_path.read_bytes()
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="SBX_000000000001",
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
    mock_backend.sandbox_status.assert_called_once_with("SBX_000000000001", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_not_called()
    mock_write_sandbox.assert_not_called()
    assert manifest_path.read_bytes() == before

def test_setup_sandbox_creates_new_when_canonical_sandbox_not_found(tmp_path):
    _write_sandbox_manifest(tmp_path, with_sandbox=True)
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SBX_000000000004"},
            }
        },
        "sandbox": {"database": "SBX_LEGACY000000"},
    }
    mock_backend = MagicMock()
    mock_backend.sandbox_status.return_value = SandboxStatusOutput(
        sandbox_database="SBX_000000000004",
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
    mock_backend.sandbox_status.assert_called_once_with("SBX_000000000004", schemas=["silver"])
    mock_backend.sandbox_reset.assert_not_called()
    mock_backend.sandbox_up.assert_called_once_with(schemas=["silver"])
    mock_write_sandbox.assert_called_once_with(tmp_path, "SBX_ABC123000000")

def test_build_sql_server_sandbox_connection_omits_driver(monkeypatch):
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox_admin")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "sandbox-password")

    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "__test_existing"},
            }
        }
    }

    updated = _build_sandbox_connection_manifest(manifest, "sql_server")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["host"] == "sandbox-host"
    assert connection["user"] == "sandbox_admin"
    assert connection["password_env"] == "SANDBOX_MSSQL_PASSWORD"
    assert "driver" not in connection

def test_build_oracle_sandbox_connection_preserves_schema_name(monkeypatch):
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1521")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "sandbox_admin")
    monkeypatch.setenv("SANDBOX_ORACLE_PASSWORD", "sandbox-password")

    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"schema_name": "SBX_000000000001"},
            }
        }
    }

    updated = _build_sandbox_connection_manifest(manifest, "oracle")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["host"] == "sandbox-host"
    assert connection["port"] == "1521"
    assert connection["service"] == "FREEPDB1"
    assert connection["schema"] == "SBX_000000000001"
    assert connection["user"] == "sandbox_admin"
    assert connection["password_env"] == "SANDBOX_ORACLE_PASSWORD"
    assert "database" not in connection

def test_write_sandbox_connection_preserves_existing_canonical_name_and_ignores_legacy(tmp_path, monkeypatch):
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "SBX_000000000001"},
            }
        },
        "sandbox": {"database": "SBX_LEGACY000000"},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "127.0.0.1")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sa")

    updated = _write_sandbox_connection_to_manifest(tmp_path, manifest, "sql_server")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["database"] == "SBX_000000000001"
    assert connection["host"] == "127.0.0.1"
    assert "driver" not in connection
    assert updated["sandbox"]["database"] == "SBX_LEGACY000000"

def test_write_oracle_sandbox_connection_preserves_existing_schema_name(tmp_path, monkeypatch):
    manifest = {
        "runtime": {
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"schema_name": "SBX_000000000001"},
            }
        },
        "sandbox": {"database": "SBX_LEGACY000000"},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "127.0.0.1")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1521")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "system")

    updated = _write_sandbox_connection_to_manifest(tmp_path, manifest, "oracle")

    connection = updated["runtime"]["sandbox"]["connection"]
    assert connection["schema"] == "SBX_000000000001"
    assert connection["host"] == "127.0.0.1"
    assert connection["service"] == "FREEPDB1"
    assert connection["password_env"] == "SANDBOX_ORACLE_PASSWORD"
    assert "database" not in connection
    assert updated["sandbox"]["database"] == "SBX_LEGACY000000"

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
    _write_sandbox_manifest(tmp_path)
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
    _write_sandbox_manifest(tmp_path)
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
    _write_sandbox_manifest(tmp_path)
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
    _write_sandbox_manifest(tmp_path)

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", side_effect=SystemExit(1)),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
