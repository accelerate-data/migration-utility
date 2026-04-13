"""Oracle integration coverage for catalog_enrich CLI."""

from __future__ import annotations

import json
import os

import oracledb
import pytest

from tests.unit.catalog_enrich.test_catalog_enrich import (
    _git_init,
    _run_enrich_cli,
    _run_setup_ddl_cli,
)

pytestmark = pytest.mark.oracle


class TestEnrichCatalogOracleIntegration:
    def _skip_if_missing(self):
        for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
            if not os.environ.get(var):
                pytest.skip(f"{var} not set")
        try:
            conn = oracledb.connect(
                user=os.environ["ORACLE_USER"],
                password=os.environ["ORACLE_PASSWORD"],
                dsn=os.environ["ORACLE_DSN"],
            )
            conn.close()
        except oracledb.Error as exc:
            pytest.skip(f"Oracle test database not reachable: {exc}")

    def test_oracle_dialect_from_manifest(self, tmp_path):
        self._skip_if_missing()
        _git_init(tmp_path)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"technology": "oracle", "dialect": "oracle"}), encoding="utf-8"
        )
        extract = _run_setup_ddl_cli([
            "extract", "--schemas", "SH", "--project-root", str(tmp_path),
        ])
        assert extract.returncode == 0, f"extract failed: {extract.stderr}"

        result = _run_enrich_cli(tmp_path, timeout=60)
        assert result.returncode == 0, f"catalog-enrich failed: {result.stderr}"
        out = json.loads(result.stdout)
        assert "tables_augmented" in out
        assert "procedures_augmented" in out
