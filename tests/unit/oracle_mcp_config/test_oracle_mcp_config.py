from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_oracle_mcp_uses_sqlcl_bin_with_sql_fallback() -> None:
    config = json.loads((REPO_ROOT / ".mcp.json").read_text(encoding="utf-8"))
    oracle = config["mcpServers"]["oracle"]

    assert oracle["command"] == "sh"
    assert oracle["args"] == [
        "-lc",
        'exec "${SQLCL_BIN:-$(command -v sql || command -v sqlcl || printf sql)}" -mcp',
    ]


def test_install_docs_explain_sqlcl_bin_override() -> None:
    text = (REPO_ROOT / "docs/wiki/Installation-and-Prerequisites.md").read_text(
        encoding="utf-8"
    )

    assert "SQLCL_BIN" in text
    assert "${SQLCL_BIN:-sql}" in text
    assert "set `SQLCL_BIN`" in text
