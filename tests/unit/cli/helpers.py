from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import shared.cli.error_handler as _err_mod


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"schema_version": "1"}), encoding="utf-8")


def _write_sandbox_manifest(tmp_path: Path, with_sandbox: bool = False) -> None:
    manifest = {
        "schema_version": "1",
        "technology": "sql_server",
        "runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {}}},
        "extraction": {"schemas": ["silver"]},
    }
    if with_sandbox:
        manifest["runtime"]["sandbox"] = {"technology": "sql_server", "dialect": "tsql", "connection": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def _patch_pyodbc_programming():
    class _FakePyodbcProgramming(Exception): pass
    return _FakePyodbcProgramming, patch.multiple(
        _err_mod,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_INTERFACE_ERROR=None,
        _PYODBC_OPERATIONAL_ERROR=None,
        _PYODBC_ERROR=_FakePyodbcProgramming,
    )
