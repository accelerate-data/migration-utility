"""Tests for global non-preserve dry-run reset behavior."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.dry_run.dry_run_test_helpers import _make_reset_project


def test_prepare_reset_migration_all_manifest_clears_runtime_and_extraction(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_global import prepare_reset_migration_all_manifest

    root = _make_reset_project(tmp_path)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    updated, cleared = prepare_reset_migration_all_manifest(root)

    assert updated is not None
    assert "runtime" not in updated
    assert "extraction" not in updated
    assert "init_handoff" not in updated
    assert cleared == [
        "runtime.source",
        "runtime.target",
        "runtime.sandbox",
        "extraction",
        "init_handoff",
    ]


def test_run_reset_migration_all_deletes_configured_paths_and_keeps_scaffold(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_global import run_reset_migration_all

    root = _make_reset_project(tmp_path)
    (root / "CLAUDE.md").write_text("# local scaffold\n", encoding="utf-8")
    (root / ".envrc").write_text("export TEST=1\n", encoding="utf-8")
    (root / "repo-map.json").write_text("{\"name\": \"fixture\"}\n", encoding="utf-8")
    (root / "ddl").mkdir()
    (root / ".staging").mkdir()
    (root / "dbt" / "target").mkdir(parents=True)
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest["runtime"] = {
        "source": {"technology": "sql_server"},
        "target": {"technology": "sql_server"},
        "sandbox": {"technology": "sql_server"},
    }
    manifest["extraction"] = {"schemas": ["silver"]}
    manifest["init_handoff"] = {"timestamp": "2026-04-01T00:00:00Z"}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = run_reset_migration_all(root)

    assert result.deleted_paths == ["catalog", "ddl", ".staging", "test-specs", "dbt"]
    assert result.missing_paths == []
    assert "runtime.source" in result.cleared_manifest_sections
    assert (root / "manifest.json").exists()
    assert (root / "CLAUDE.md").exists()
    assert (root / ".envrc").exists()
    assert (root / "repo-map.json").exists()
    assert not (root / "catalog").exists()
