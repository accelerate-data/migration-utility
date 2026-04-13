from __future__ import annotations

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_DIR.parents[4]
_DDL_MCP_DIR = _REPO_ROOT / "plugin" / "mcp" / "ddl"

if str(_DDL_MCP_DIR) not in sys.path:
    sys.path.insert(0, str(_DDL_MCP_DIR))
