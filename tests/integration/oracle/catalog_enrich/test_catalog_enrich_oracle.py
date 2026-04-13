"""Oracle integration coverage for catalog_enrich CLI."""

from __future__ import annotations

import json

import pytest

from tests.helpers import (
    ORACLE_MIGRATION_SCHEMA,
    git_init,
    run_catalog_enrich_cli,
    run_setup_ddl_cli,
)

pytestmark = pytest.mark.oracle


@pytest.mark.usefixtures("oracle_extract_env")
class TestEnrichCatalogOracleIntegration:
    def test_oracle_dialect_from_manifest(self, tmp_path, oracle_extract_env):
        git_init(tmp_path)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"technology": "oracle", "dialect": "oracle"}), encoding="utf-8"
        )
        extract = run_setup_ddl_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ])
        assert extract.returncode == 0, f"extract failed: {extract.stderr}"

        result = run_catalog_enrich_cli(tmp_path, timeout=60)
        assert result.returncode == 0, f"catalog-enrich failed: {result.stderr}"
        out = json.loads(result.stdout)
        assert "tables_augmented" in out
        assert "procedures_augmented" in out
