# Oracle MCP Fallback And DDL MCP Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Oracle MCP launch use `SQLCL_BIN` with `sql` on `PATH` as the fallback, and narrow `ddl-mcp` so it no longer depends on the broad editable `shared` package.

**Architecture:** Replace the Oracle MCP command in `.mcp.json` with a shell invocation that expands `SQLCL_BIN` and falls back to `sql`. For `ddl-mcp`, copy only the small DDL loader support slice that `mcp/ddl/server.py` needs into `mcp/ddl/`, update imports to use the local package, and drop the editable `shared` dependency from the MCP package.

**Tech Stack:** JSON config, bash shell expansion, Python 3.11, uv, pytest, sqlglot, MCP Python SDK

---

## Task 1: Lock Oracle MCP Fallback Behavior With Tests

**Files:**

- Create: `tests/unit/oracle_mcp_config/test_oracle_mcp_config.py`
- Modify: `.mcp.json`
- Test: `tests/unit/oracle_mcp_config/test_oracle_mcp_config.py`

- [ ] **Step 1: Write the failing test**

```python
from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_oracle_mcp_uses_sqlcl_bin_with_sql_fallback() -> None:
    config = json.loads((REPO_ROOT / ".mcp.json").read_text(encoding="utf-8"))
    oracle = config["mcpServers"]["oracle"]

    assert oracle["command"] == "sh"
    assert oracle["args"] == ["-lc", 'exec "${SQLCL_BIN:-sql}" -mcp']
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`
Expected: FAIL because `.mcp.json` still uses `"command": "sql"`.

- [ ] **Step 3: Write minimal implementation**

```json
"oracle": {
  "command": "sh",
  "args": [
    "-lc",
    "exec \"${SQLCL_BIN:-sql}\" -mcp"
  ]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add .mcp.json tests/unit/oracle_mcp_config/test_oracle_mcp_config.py
git commit -m "test: lock Oracle MCP SQLCL fallback config"
```

## Task 2: Document The Oracle Override And Failure Path

**Files:**

- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Test: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`

- [ ] **Step 1: Write the failing documentation-focused test**

```python
def test_install_docs_explain_sqlcl_bin_override() -> None:
    text = (REPO_ROOT / "docs/wiki/Installation-and-Prerequisites.md").read_text(
        encoding="utf-8"
    )

    assert "SQLCL_BIN" in text
    assert "${SQLCL_BIN:-sql}" in text
    assert "set `SQLCL_BIN`" in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`
Expected: FAIL because the docs only mention `sql` on `PATH` and local `.mcp.json` overrides.

- [ ] **Step 3: Write minimal implementation**

```md
| [SQLcl](https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/) (`sql`) | Oracle source projects | CLI tool that provides the Oracle MCP server via `sql -mcp`; requires Java 11+. The plugin launches Oracle through `exec "${SQLCL_BIN:-sql}" -mcp`. Set `SQLCL_BIN` when SQLcl is not on `PATH` or another `sql` binary conflicts. |
```

```md
If Oracle MCP fails to start because `sql` cannot be found or the wrong binary is
picked up from `PATH`, set `SQLCL_BIN` to the SQLcl executable before launching
Claude Code.
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add docs/wiki/Installation-and-Prerequisites.md tests/unit/oracle_mcp_config/test_oracle_mcp_config.py
git commit -m "docs: explain Oracle SQLCL_BIN override"
```

## Task 3: Lock The Narrow DDL MCP Dependency Boundary With Tests

**Files:**

- Modify: `mcp/ddl/pyproject.toml`
- Modify: `mcp/ddl/tests/unit/test_server.py`
- Test: `mcp/ddl/tests/unit/test_server.py`

- [ ] **Step 1: Write the failing tests**

```python
def test_ddl_mcp_pyproject_does_not_depend_on_shared_package() -> None:
    pyproject = (REPO_ROOT / "mcp/ddl/pyproject.toml").read_text(encoding="utf-8")

    assert '"shared"' not in pyproject
    assert "shared = { path = \"../../lib\"" not in pyproject
```

```python
def test_ddl_server_imports_local_support_package() -> None:
    text = (REPO_ROOT / "mcp/ddl/server.py").read_text(encoding="utf-8")

    assert "from ddl_mcp_support" in text
    assert "from shared." not in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd mcp/ddl && uv run pytest tests/unit/test_server.py -v`
Expected: FAIL because `mcp/ddl` still depends on `shared` and `server.py` imports `shared.*`.

- [ ] **Step 3: Write minimal implementation**

```toml
dependencies = [
    "mcp>=1.0",
    "oracledb>=3.0",
    "pytest>=9.0.3",
    "sqlglot>=25.0,<26",
]
```

```python
from ddl_mcp_support.env_config import assert_git_repo
from ddl_mcp_support.loader import (
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    extract_refs,
    load_directory,
    read_manifest,
)
from ddl_mcp_support.name_resolver import normalize
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd mcp/ddl && uv run pytest tests/unit/test_server.py -v`
Expected: PASS for the new dependency-boundary assertions, with any broken imports exposed for the next task.

- [ ] **Step 5: Commit**

```bash
git add mcp/ddl/pyproject.toml mcp/ddl/tests/unit/test_server.py mcp/ddl/server.py
git commit -m "test: lock ddl-mcp local dependency boundary"
```

## Task 4: Localize The DDL Support Slice Used By The MCP Server

**Files:**

- Create: `mcp/ddl/ddl_mcp_support/__init__.py`
- Create: `mcp/ddl/ddl_mcp_support/env_config.py`
- Create: `mcp/ddl/ddl_mcp_support/name_resolver.py`
- Create: `mcp/ddl/ddl_mcp_support/tsql_utils.py`
- Create: `mcp/ddl/ddl_mcp_support/routing.py`
- Create: `mcp/ddl/ddl_mcp_support/block_segmenter.py`
- Create: `mcp/ddl/ddl_mcp_support/loader_data.py`
- Create: `mcp/ddl/ddl_mcp_support/loader_parse.py`
- Create: `mcp/ddl/ddl_mcp_support/loader_io.py`
- Create: `mcp/ddl/ddl_mcp_support/loader.py`
- Modify: `mcp/ddl/server.py`
- Modify: `mcp/ddl/tests/unit/test_server.py`
- Test: `mcp/ddl/tests/unit/test_server.py`

- [ ] **Step 1: Write the minimal local package files**

```python
"""Local support modules for the standalone ddl-mcp package."""
```

```python
from ddl_mcp_support.loader_data import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    ObjectNotFoundError,
    ObjectRefs,
    ProfileMissingError,
)
from ddl_mcp_support.loader_io import load_directory, read_manifest
from ddl_mcp_support.loader_parse import extract_refs
```

- [ ] **Step 2: Port only the direct support code**

```python
from ddl_mcp_support.name_resolver import normalize
```

```python
from ddl_mcp_support.tsql_utils import mask_tsql
```

```python
from ddl_mcp_support.block_segmenter import segment_sql
from ddl_mcp_support.routing import scan_routing_flags
```

Write the local modules by copying only the current logic used by the DDL MCP server and its unit tests. Do not port unrelated runtime-config, catalog-write, or CLI code.

- [ ] **Step 3: Run the unit suite**

Run: `cd mcp/ddl && uv run pytest tests/unit/test_server.py -v`
Expected: PASS

- [ ] **Step 4: Run the full ddl-mcp suite**

Run: `cd mcp/ddl && uv run pytest`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mcp/ddl/ddl_mcp_support mcp/ddl/server.py mcp/ddl/tests/unit/test_server.py mcp/ddl/pyproject.toml mcp/ddl/uv.lock
git commit -m "feat: make ddl-mcp self-contained"
```

## Task 5: Final Verification And Issue Handoff

**Files:**

- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `.mcp.json`
- Modify: `mcp/ddl/**`
- Test: `tests/unit/oracle_mcp_config/test_oracle_mcp_config.py`
- Test: `mcp/ddl/tests/unit/test_server.py`

- [ ] **Step 1: Run changed-area tests**

Run: `cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v`
Expected: PASS

Run: `cd mcp/ddl && uv run pytest`
Expected: PASS

- [ ] **Step 2: Run a smoke check for the Oracle command shape**

Run: `python - <<'PY'
import json
from pathlib import Path
cfg = json.loads(Path('.mcp.json').read_text())
print(cfg["mcpServers"]["oracle"])
PY`
Expected: prints the `sh -lc` command using `SQLCL_BIN` fallback.

- [ ] **Step 3: Update Linear implementation notes**

Post:

```text
Implemented Oracle MCP startup fallback via SQLCL_BIN -> sql and narrowed ddl-mcp to a local dependency slice so its package no longer rides the editable shared package. Verification: cd lib && uv run pytest ../tests/unit/oracle_mcp_config/test_oracle_mcp_config.py -v; cd mcp/ddl && uv run pytest.
```

- [ ] **Step 4: Create the final commit**

```bash
git add .mcp.json docs/wiki/Installation-and-Prerequisites.md mcp/ddl tests/unit/oracle_mcp_config/test_oracle_mcp_config.py
git commit -m "feat: harden Oracle MCP startup and narrow ddl-mcp boundary"
```

- [ ] **Step 5: Leave the worktree clean**

Run: `git status --short`
Expected: no output

No manual tests required.
