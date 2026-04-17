"""Tests for catalog_diff.py — diff-aware classification of catalog objects."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from shared.catalog_diff import (
    classify_objects,
    compute_object_hashes,
    load_existing_hashes,
)
from shared.ddl_hash import hash_definition, hash_table_signals


# ── classify_objects ─────────────────────────────────────────────────────────


class TestClassifyObjects:
    def test_all_new(self) -> None:
        fresh = {"dbo.t1": "aaa", "dbo.usp_a": "bbb"}
        existing: dict[str, str | None] = {}
        result = classify_objects(fresh, existing)
        assert result.new == {"dbo.t1", "dbo.usp_a"}
        assert result.changed == set()
        assert result.unchanged == set()
        assert result.removed == set()

    def test_all_unchanged(self) -> None:
        fresh = {"dbo.t1": "aaa", "dbo.usp_a": "bbb"}
        existing = {"dbo.t1": "aaa", "dbo.usp_a": "bbb"}
        result = classify_objects(fresh, existing)
        assert result.unchanged == {"dbo.t1", "dbo.usp_a"}
        assert result.changed == set()
        assert result.new == set()
        assert result.removed == set()

    def test_changed_hash(self) -> None:
        fresh = {"dbo.t1": "aaa"}
        existing = {"dbo.t1": "zzz"}
        result = classify_objects(fresh, existing)
        assert result.changed == {"dbo.t1"}
        assert result.unchanged == set()

    def test_existing_none_hash_treated_as_changed(self) -> None:
        fresh = {"dbo.t1": "aaa"}
        existing: dict[str, str | None] = {"dbo.t1": None}
        result = classify_objects(fresh, existing)
        assert result.changed == {"dbo.t1"}

    def test_removed(self) -> None:
        fresh: dict[str, str] = {}
        existing = {"dbo.old_table": "aaa"}
        result = classify_objects(fresh, existing)
        assert result.removed == {"dbo.old_table"}

    def test_mixed(self) -> None:
        fresh = {"dbo.t1": "aaa", "dbo.t2": "changed", "dbo.t3": "new_hash"}
        existing = {"dbo.t1": "aaa", "dbo.t2": "old_hash", "dbo.gone": "xxx"}
        result = classify_objects(fresh, existing)
        assert result.unchanged == {"dbo.t1"}
        assert result.changed == {"dbo.t2"}
        assert result.new == {"dbo.t3"}
        assert result.removed == {"dbo.gone"}


# ── compute_object_hashes ────────────────────────────────────────────────────


class TestComputeObjectHashes:
    def test_hashes_definitions(self) -> None:
        definitions_rows = [
            {"schema_name": "dbo", "object_name": "usp_a", "definition": "CREATE PROC dbo.usp_a AS SELECT 1"},
        ]
        hashes = compute_object_hashes(definitions_rows, {}, {})
        assert "dbo.usp_a" in hashes
        assert hashes["dbo.usp_a"] == hash_definition("CREATE PROC dbo.usp_a AS SELECT 1")

    def test_hashes_table_signals(self) -> None:
        signals = {"dbo.t1": {"columns": [{"name": "id"}], "primary_keys": []}}
        hashes = compute_object_hashes([], signals, {})
        assert "dbo.t1" in hashes
        assert hashes["dbo.t1"] == hash_table_signals(signals["dbo.t1"])

    def test_skips_null_definitions(self) -> None:
        rows = [{"schema_name": "dbo", "object_name": "usp_a", "definition": None}]
        hashes = compute_object_hashes(rows, {}, {})
        assert "dbo.usp_a" not in hashes

    def test_combined(self) -> None:
        definitions = [
            {"schema_name": "dbo", "object_name": "usp_a", "definition": "CREATE PROC AS SELECT 1"},
        ]
        signals = {"dbo.t1": {"columns": []}}
        hashes = compute_object_hashes(definitions, signals, {})
        assert "dbo.usp_a" in hashes
        assert "dbo.t1" in hashes


# ── load_existing_hashes ─────────────────────────────────────────────────────


class TestLoadExistingHashes:
    def test_empty_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = load_existing_hashes(Path(tmp))
            assert result == {}

    def test_reads_ddl_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "catalog" / "procedures"
            p.mkdir(parents=True)
            (p / "dbo.usp_a.json").write_text(
                json.dumps({"schema": "dbo", "name": "usp_a", "ddl_hash": "abc123"})
            )
            result = load_existing_hashes(Path(tmp))
            assert result["dbo.usp_a"] == "abc123"

    def test_missing_ddl_hash_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "catalog" / "tables"
            p.mkdir(parents=True)
            (p / "dbo.t1.json").write_text(
                json.dumps({"schema": "dbo", "name": "t1"})
            )
            result = load_existing_hashes(Path(tmp))
            assert result["dbo.t1"] is None

    def test_multiple_buckets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for bucket in ("tables", "procedures", "views"):
                d = Path(tmp) / "catalog" / bucket
                d.mkdir(parents=True)
            (Path(tmp) / "catalog" / "tables" / "dbo.t1.json").write_text(
                json.dumps({"ddl_hash": "h1"})
            )
            (Path(tmp) / "catalog" / "procedures" / "dbo.usp_a.json").write_text(
                json.dumps({"ddl_hash": "h2"})
            )
            result = load_existing_hashes(Path(tmp))
            assert result["dbo.t1"] == "h1"
            assert result["dbo.usp_a"] == "h2"

    def test_corrupt_file_returns_none_and_continues(self) -> None:
        """Corrupt JSON files produce None hash; other files are unaffected."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "catalog" / "procedures"
            p.mkdir(parents=True)
            (p / "dbo.corrupt.json").write_text("{truncated", encoding="utf-8")
            (p / "dbo.good.json").write_text(json.dumps({"ddl_hash": "abc"}))
            result = load_existing_hashes(Path(tmp))
            assert result["dbo.corrupt"] is None
            assert result["dbo.good"] == "abc"
