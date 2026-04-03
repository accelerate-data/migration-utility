"""Tests for ddl_hash.py — deterministic DDL hashing."""

from __future__ import annotations

from shared.ddl_hash import hash_definition, hash_table_signals, normalize_definition


class TestNormalizeDefinition:
    def test_collapses_whitespace(self) -> None:
        raw = "CREATE  PROC   dbo.usp_a\n  AS\n    SELECT  1"
        assert normalize_definition(raw) == "create proc dbo.usp_a as select 1"

    def test_strips_leading_trailing(self) -> None:
        raw = "  SELECT 1  "
        assert normalize_definition(raw) == "select 1"

    def test_tabs_and_newlines(self) -> None:
        raw = "CREATE\tPROC\r\ndbo.usp_a\nAS\nSELECT 1"
        assert normalize_definition(raw) == "create proc dbo.usp_a as select 1"

    def test_preserves_comments(self) -> None:
        raw = "CREATE PROC dbo.usp_a AS -- important comment\nSELECT 1"
        normalized = normalize_definition(raw)
        assert "-- important comment" in normalized

    def test_empty_string(self) -> None:
        assert normalize_definition("") == ""

    def test_deterministic_across_whitespace_variants(self) -> None:
        v1 = "CREATE PROC dbo.usp_a AS SELECT 1"
        v2 = "CREATE  PROC  dbo.usp_a  AS  SELECT  1"
        v3 = "CREATE\nPROC\ndbo.usp_a\nAS\nSELECT\n1"
        assert normalize_definition(v1) == normalize_definition(v2) == normalize_definition(v3)


class TestHashDefinition:
    def test_deterministic(self) -> None:
        definition = "CREATE PROC dbo.usp_a AS SELECT 1"
        h1 = hash_definition(definition)
        h2 = hash_definition(definition)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_whitespace_variant_same_hash(self) -> None:
        h1 = hash_definition("CREATE PROC dbo.usp_a AS SELECT 1")
        h2 = hash_definition("CREATE  PROC\n dbo.usp_a  AS\n SELECT  1")
        assert h1 == h2

    def test_different_definitions_different_hash(self) -> None:
        h1 = hash_definition("CREATE PROC dbo.usp_a AS SELECT 1")
        h2 = hash_definition("CREATE PROC dbo.usp_a AS SELECT 2")
        assert h1 != h2


class TestHashTableSignals:
    def test_deterministic(self) -> None:
        signals = {"columns": [{"name": "id", "sql_type": "INT"}], "primary_keys": []}
        h1 = hash_table_signals(signals)
        h2 = hash_table_signals(signals)
        assert h1 == h2
        assert len(h1) == 64

    def test_key_order_irrelevant(self) -> None:
        s1 = {"columns": [], "primary_keys": []}
        s2 = {"primary_keys": [], "columns": []}
        assert hash_table_signals(s1) == hash_table_signals(s2)

    def test_different_signals_different_hash(self) -> None:
        s1 = {"columns": [{"name": "id"}]}
        s2 = {"columns": [{"name": "name"}]}
        assert hash_table_signals(s1) != hash_table_signals(s2)
