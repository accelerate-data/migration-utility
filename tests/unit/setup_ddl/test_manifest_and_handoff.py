"""Tests for partial manifest, handoff, list-databases/schemas guards, and connection identity."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from tests.helpers import run_setup_ddl_cli as _run_cli

from .conftest import _write_json


# ── Unit: write-partial-manifest ────────────────────────────────────────────


class TestWritePartialManifest:
    def test_writes_partial_manifest(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["schema_version"] == "1.0"
        assert manifest["technology"] == "oracle"
        assert manifest["dialect"] == "oracle"
        assert manifest["runtime"]["source"]["technology"] == "oracle"
        assert manifest["runtime"]["source"]["dialect"] == "oracle"
        assert manifest["runtime"]["sandbox"]["technology"] == "oracle"
        assert manifest["runtime"]["sandbox"]["dialect"] == "oracle"
        assert manifest["runtime"]["source"]["connection"] == {}
        assert manifest["runtime"]["sandbox"]["connection"] == {}
        assert "extraction" not in manifest

    def test_partial_manifest_sql_server(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--target-technology", "sql_server",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["technology"] == "sql_server"
        assert manifest["dialect"] == "tsql"
        assert manifest["runtime"]["source"]["technology"] == "sql_server"
        assert manifest["runtime"]["sandbox"]["technology"] == "sql_server"

    def test_partial_manifest_can_seed_distinct_target_role(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "sql_server",
        ])
        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["source"]["technology"] == "oracle"
        assert manifest["runtime"]["sandbox"]["technology"] == "oracle"
        assert manifest["runtime"]["target"]["technology"] == "sql_server"
        assert manifest["runtime"]["target"]["dialect"] == "tsql"

    def test_partial_manifest_invalid_technology(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "postgres",
            "--target-technology", "oracle",
        ])
        assert result.returncode != 0

    def test_partial_manifest_invalid_target_technology(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "postgres",
        ])
        assert result.returncode != 0

    def test_full_manifest_enriches_partial(self, tmp_path):
        # Write partial first
        _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])
        # Then enrich with full manifest
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "FREEPDB1",
            "--schemas", "SH,HR",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        # All fields present
        assert manifest["technology"] == "oracle"
        assert manifest["dialect"] == "oracle"
        assert manifest["runtime"]["source"]["connection"]["schema"] == "FREEPDB1"
        assert manifest["extraction"]["schemas"] == ["SH", "HR"]
        assert "extracted_at" in manifest["extraction"]

    def test_full_manifest_preserves_existing_init_handoff(self, tmp_path):
        prereqs_payload = {
            "common": {
                "startup": {
                    "uv": True,
                    "python": True,
                }
            },
            "roles": {
                "source": {
                    "technology": "oracle",
                    "startup": {
                        "oracledb": True,
                    },
                },
                "sandbox": {
                    "technology": "oracle",
                    "startup": {
                        "oracledb": True,
                    },
                },
                "target": {
                    "technology": "sql_server",
                    "startup": {
                        "pyodbc": True,
                    },
                },
            },
        }
        prereqs = json.dumps(prereqs_payload)
        _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "sql_server",
            "--prereqs-json", prereqs,
        ])

        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "FREEPDB1",
            "--schemas", "SH,HR",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        persisted_handoff = dict(manifest["init_handoff"])
        persisted_handoff.pop("timestamp")
        assert persisted_handoff == prereqs_payload
        assert "timestamp" in manifest["init_handoff"]

    def test_partial_manifest_scrubs_stale_unsupported_runtime_roles(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "runtime": {
                        "sandbox": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/sandbox.duckdb"},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["source"]["technology"] == "oracle"
        assert manifest["runtime"]["sandbox"]["technology"] == "oracle"
        assert manifest["runtime"]["target"]["technology"] == "oracle"
        assert manifest["technology"] == "oracle"

    def test_partial_manifest_preserves_supported_existing_target_role(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "target": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TargetDB"},
                            "schemas": {"source": "bronze"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "sql_server",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["target"]["connection"]["database"] == "TargetDB"
        assert manifest["runtime"]["target"]["schemas"]["source"] == "bronze"

    def test_partial_manifest_replaces_existing_target_when_technology_changes(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "target": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TargetDB"},
                            "schemas": {"source": "bronze"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["target"]["technology"] == "oracle"
        assert manifest["runtime"]["target"]["connection"] == {}
        assert "schemas" not in manifest["runtime"]["target"]

    def test_partial_manifest_preserves_existing_source_and_sandbox_connection_state(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SRCPDB", "schema": "BRONZE"},
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {"service": "SANDBOXPDB", "schema": "TESTING"},
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["source"]["connection"]["service"] == "SRCPDB"
        assert manifest["runtime"]["source"]["connection"]["schema"] == "BRONZE"
        assert manifest["runtime"]["sandbox"]["connection"]["service"] == "SANDBOXPDB"
        assert manifest["runtime"]["sandbox"]["connection"]["schema"] == "TESTING"


# ── Unit: write-partial-manifest with prereqs ───────────────────────────────


class TestWritePartialManifestHandoff:
    def test_prereqs_json_writes_init_handoff(self, tmp_path):
        prereqs = json.dumps({
            "common": {
                "startup": {
                    "uv": True,
                    "python": True,
                    "shared_deps": True,
                    "ddl_mcp": True,
                }
            },
            "roles": {
                "source": {
                    "technology": "sql_server",
                    "startup": {
                        "freetds": True,
                        "toolbox": False,
                        "driver_override_resolved": True,
                    },
                },
                "sandbox": {
                    "technology": "sql_server",
                    "startup": {
                        "freetds": True,
                    },
                },
                "target": {
                    "technology": "oracle",
                    "startup": {
                        "oracledb": True,
                    },
                },
            },
        })
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--target-technology", "oracle",
            "--prereqs-json", prereqs,
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "init_handoff" in manifest
        handoff = manifest["init_handoff"]
        assert handoff["common"]["startup"]["uv"] is True
        assert handoff["roles"]["source"]["technology"] == "sql_server"
        assert handoff["roles"]["source"]["startup"]["freetds"] is True
        assert handoff["roles"]["target"]["technology"] == "oracle"
        assert handoff["roles"]["target"]["startup"]["oracledb"] is True
        assert "timestamp" in handoff

    def test_without_prereqs_json_no_init_handoff(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--target-technology", "oracle",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "init_handoff" not in manifest

    def test_invalid_prereqs_json_fails(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--prereqs-json", "{not valid json}",
        ])
        assert result.returncode != 0


# ── Unit: read-handoff ──────────────────────────────────────────────────────


class TestReadHandoff:
    def test_handoff_present_returns_skip_true(self, tmp_path):
        manifest = {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
            "init_handoff": {
                "timestamp": "2026-04-01T00:00:00+00:00",
                "env_vars": {"MSSQL_HOST": True},
                "tools": {"uv": True},
            },
        }
        _write_json(tmp_path / "manifest.json", manifest)
        result = _run_cli(["read-handoff", "--project-root", str(tmp_path)])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["skip"] is True
        assert out["handoff"]["env_vars"]["MSSQL_HOST"] is True

    def test_missing_handoff_returns_skip_false(self, tmp_path):
        manifest = {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
        }
        _write_json(tmp_path / "manifest.json", manifest)
        result = _run_cli(["read-handoff", "--project-root", str(tmp_path)])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["skip"] is False

    def test_no_manifest_returns_skip_false(self, tmp_path):
        result = _run_cli(["read-handoff", "--project-root", str(tmp_path)])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["skip"] is False


# ── Unit: list-databases guards ──────────────────────────────────────────────


class TestListDatabasesGuards:
    def test_missing_manifest_fails(self, tmp_path):
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "manifest" in result.stderr.lower() or "manifest" in result.stdout.lower()

    def test_missing_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": "1.0"}', encoding="utf-8")
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0

    def test_unsupported_runtime_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "runtime": {
                        "source": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/source.duckdb"},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "supported runtime technology" in result.stderr.lower() or "supported runtime technology" in result.stdout.lower()

    def test_unsupported_top_level_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "duckdb", "dialect": "duckdb"}',
            encoding="utf-8",
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "supported runtime technology" in result.stderr.lower() or "supported runtime technology" in result.stdout.lower()

    def test_unsupported_non_source_runtime_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "runtime": {
                        "target": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/target.duckdb"},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "supported runtime technology" in result.stderr.lower() or "supported runtime technology" in result.stdout.lower()

    def test_oracle_unsupported(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "oracle" in result.stderr.lower()


# ── Unit: list-schemas guards ────────────────────────────────────────────────


class TestListSchemasGuards:
    def test_missing_manifest_fails(self, tmp_path):
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "manifest" in result.stderr.lower() or "manifest" in result.stdout.lower()

    def test_missing_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": "1.0"}', encoding="utf-8")
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0

    def test_sql_server_requires_database_arg(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "database" in result.stderr.lower()


# ── Unit: connection identity ─────────────────────────────────────────────────


def test_sql_server_connection_identity_omits_driver(monkeypatch):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
    from shared.setup_ddl_support.manifest import get_connection_identity

    monkeypatch.setenv("SOURCE_MSSQL_HOST", "server1.example.com")
    monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "warehouse_user")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")
    monkeypatch.setenv("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    identity = get_connection_identity("sql_server", "WarehouseDb")

    assert identity["connection"] == {
        "host": "server1.example.com",
        "port": "1433",
        "database": "WarehouseDb",
        "user": "warehouse_user",
        "password_env": "SOURCE_MSSQL_PASSWORD",
    }
    assert "driver" not in identity["connection"]


class TestConnectionIdentity:
    """Tests for setup-ddl source identity and stale-marking helpers."""

    @staticmethod
    def _import():
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale
        from shared.setup_ddl_support.manifest import (
            get_connection_identity,
            identity_changed,
        )
        return get_connection_identity, identity_changed, mark_all_catalog_stale

    def test_sqlserver_identity_reads_env(self, monkeypatch):
        get_connection_identity, _, _ = self._import()
        monkeypatch.setenv("SOURCE_MSSQL_HOST", "server1.example.com")
        monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
        identity = get_connection_identity("sql_server", "AdventureWorks")
        assert identity["connection"]["host"] == "server1.example.com"
        assert identity["connection"]["port"] == "1433"
        assert identity["connection"]["database"] == "AdventureWorks"
        assert identity["connection"]["password_env"] == "SOURCE_MSSQL_PASSWORD"

    def test_oracle_identity_reads_dsn(self, monkeypatch):
        get_connection_identity, _, _ = self._import()
        for env_var in (
            "SOURCE_ORACLE_HOST",
            "SOURCE_ORACLE_PORT",
            "SOURCE_ORACLE_SERVICE",
            "SOURCE_ORACLE_USER",
            "SOURCE_ORACLE_SCHEMA",
        ):
            monkeypatch.delenv(env_var, raising=False)
        monkeypatch.setenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
        identity = get_connection_identity("oracle", "")
        assert identity["connection"]["dsn"] == "localhost:1521/FREEPDB1"
        assert "host" not in identity["connection"]
        assert identity["connection"]["password_env"] == "SOURCE_ORACLE_PASSWORD"

    def test_sqlserver_manifest_stores_identity(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SOURCE_MSSQL_HOST", "db1.internal")
        monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--database", "MyDB",
            "--schemas", "silver",
        ])
        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["source"]["connection"]["host"] == "db1.internal"
        assert manifest["runtime"]["source"]["connection"]["port"] == "1433"
        assert manifest["runtime"]["source"]["connection"]["database"] == "MyDB"
        assert manifest["runtime"]["source"]["connection"]["password_env"] == "SOURCE_MSSQL_PASSWORD"

    def test_oracle_manifest_stores_dsn(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ORACLE_DSN", "oraclehost:1521/PROD")
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "PROD",
            "--schemas", "SH",
        ])
        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["source"]["connection"]["dsn"] == "oraclehost:1521/PROD"
        assert manifest["runtime"]["source"]["connection"]["password_env"] == "SOURCE_ORACLE_PASSWORD"

    def test_identity_changed_host(self):
        _, identity_changed_fn, _ = self._import()
        existing = {"runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {"host": "old-server", "port": "1433", "database": "DB1"}}}}
        current = {"connection": {"host": "new-server", "port": "1433", "database": "DB1"}}
        assert identity_changed_fn(existing, current) is True

    def test_identity_changed_database(self):
        _, identity_changed_fn, _ = self._import()
        existing = {"runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {"host": "server1", "port": "1433", "database": "DB1"}}}}
        current = {"connection": {"host": "server1", "port": "1433", "database": "DB2"}}
        assert identity_changed_fn(existing, current) is True

    def test_identity_unchanged(self):
        _, identity_changed_fn, _ = self._import()
        existing = {"runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {"host": "server1", "port": "1433", "database": "DB1"}}}}
        current = {"connection": {"host": "server1", "port": "1433", "database": "DB1"}}
        assert identity_changed_fn(existing, current) is False

    def test_identity_changed_oracle_dsn(self):
        _, identity_changed_fn, _ = self._import()
        existing = {"runtime": {"source": {"technology": "oracle", "dialect": "oracle", "connection": {"dsn": "host1:1521/SVC1"}}}}
        current = {"connection": {"dsn": "host2:1521/SVC1"}}
        assert identity_changed_fn(existing, current) is True

    def test_identity_missing_env_no_false_positive(self):
        _, identity_changed_fn, _ = self._import()
        # No current env values (all empty) — must not trigger stale flush
        existing = {"runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {"host": "server1", "port": "1433", "database": "DB1"}}}}
        current = {"connection": {"host": "", "port": "", "database": "DB1"}}
        # source_database is non-empty and matches — no change
        assert identity_changed_fn(existing, current) is False

    def test_identity_empty_env_vars_no_false_positive(self):
        _, identity_changed_fn, _ = self._import()
        existing = {"runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {"host": "server1", "port": "1433", "database": "DB1"}}}}
        # All identity values empty → treat as absent, no false positive
        current = {"connection": {"host": "", "port": ""}}
        assert identity_changed_fn(existing, current) is False

    def test_mark_all_catalog_stale(self, tmp_path):
        _, _, mark_all_catalog_stale_fn = self._import()
        # resolve_catalog_dir returns project_root / "catalog" by default (no env override in tests).
        # Seed catalog with one proc and one table (neither stale)
        proc_path = tmp_path / "catalog" / "procedures" / "dbo.usp_load.json"
        table_path = tmp_path / "catalog" / "tables" / "silver.dimcustomer.json"
        proc_path.parent.mkdir(parents=True)
        table_path.parent.mkdir(parents=True)
        proc_path.write_text(json.dumps({"ddl_hash": "abc"}), encoding="utf-8")
        table_path.write_text(json.dumps({"ddl_hash": "xyz"}), encoding="utf-8")

        written_paths = mark_all_catalog_stale_fn(tmp_path)

        assert json.loads(proc_path.read_text())["stale"] is True
        assert json.loads(table_path.read_text())["stale"] is True
        assert written_paths == [
            "catalog/tables/silver.dimcustomer.json",
            "catalog/procedures/dbo.usp_load.json",
        ]

    def test_identity_changed_pre_marks_all_stale_on_reextract(self, tmp_path, monkeypatch):
        """Identity change causes all existing catalog files to be pre-marked stale."""
        # Seed an existing manifest with old host
        manifest = {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "database": "DB1",
                        "host": "old-server",
                        "port": "1433",
                    },
                }
            },
            "extraction": {
                "schemas": ["silver"],
                "extracted_at": "2025-01-01T00:00:00+00:00",
            },
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        # Seed an existing catalog proc
        proc_path = tmp_path / "catalog" / "procedures" / "silver.usp_load.json"
        proc_path.parent.mkdir(parents=True)
        proc_path.write_text(json.dumps({"ddl_hash": "abc"}), encoding="utf-8")

        # New env points to a different host
        monkeypatch.setenv("SOURCE_MSSQL_HOST", "new-server")
        monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")

        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale
        from shared.setup_ddl_support.manifest import get_connection_identity, identity_changed

        current_identity = get_connection_identity("sql_server", "DB1")
        assert identity_changed(manifest, current_identity) is True
        written_paths = mark_all_catalog_stale(tmp_path)

        assert json.loads(proc_path.read_text())["stale"] is True
        assert written_paths == ["catalog/procedures/silver.usp_load.json"]

    def test_same_identity_no_spurious_stale(self, tmp_path, monkeypatch):
        """Same host+port+database leaves existing catalog files untouched."""
        manifest = {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "database": "DB1",
                        "host": "server1",
                        "port": "1433",
                    },
                }
            },
            "extraction": {
                "schemas": ["silver"],
                "extracted_at": "2025-01-01T00:00:00+00:00",
            },
        }
        (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        monkeypatch.setenv("SOURCE_MSSQL_HOST", "server1")
        monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")

        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
        from shared.setup_ddl_support.manifest import get_connection_identity, identity_changed

        current_identity = get_connection_identity("sql_server", "DB1")
        assert identity_changed(manifest, current_identity) is False
