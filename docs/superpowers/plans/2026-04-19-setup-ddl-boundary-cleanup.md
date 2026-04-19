# Setup DDL Boundary Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split setup-ddl extraction and manifest helpers into focused modules without changing public CLI behavior or existing import paths.

**Architecture:** Keep `shared.setup_ddl_support.extract` and `shared.setup_ddl_support.manifest` as compatibility facades. Move implementation into responsibility-focused sibling modules, add import-boundary tests first, then verify CLI-level setup-ddl behavior through the existing pytest suite.

**Tech Stack:** Python 3.11, Typer, Pydantic v2, pytest, uv, repo-local setup-ddl support modules.

---

## Worktree

Coordinator worktree:

```text
/Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
```

All implementation, tests, verification, and commits for this plan must happen under that worktree or isolated worker worktrees created for this branch. Do not edit `/Users/hbanerjee/src/migration-utility` for this work.

## Parallel Execution Model

Tasks 1 and 2 can run in parallel because their production write scopes are disjoint.

- Task 1 owner writes only extraction-support files and extraction tests.
- Task 2 owner writes only manifest-support files and manifest tests.
- Task 3 owner integrates both task results, updates `repo-map.json` if module responsibilities changed, and runs combined verification.

If using parallel implementation subagents, give each subagent the full text of only its task and these global constraints:

```text
Repo root: /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
Read AGENTS.md and repo-map.json before editing.
Do not edit files outside your assigned Files section.
Preserve public imports from shared.setup_ddl_support.extract and shared.setup_ddl_support.manifest.
Use apply_patch for manual edits.
Run the task-specific tests before reporting DONE.
Commit only your assigned files with git add <file>.
```

Use two-stage review after each implementation task:

1. Spec compliance review: verify the task followed this plan and did not edit outside scope.
2. Code quality review: verify small-module boundaries, import compatibility, and test quality.

## File Structure

### Task 1: Extraction Boundary Split

- Create: `lib/shared/setup_ddl_support/assembly.py`
- Create: `lib/shared/setup_ddl_support/discovery.py`
- Create: `lib/shared/setup_ddl_support/db_extraction.py`
- Create: `lib/shared/setup_ddl_support/extract_orchestration.py`
- Modify: `lib/shared/setup_ddl_support/extract.py`
- Test: `tests/unit/setup_ddl/test_extract_boundaries.py`
- Existing tests to run: `tests/unit/setup_ddl/test_assembly.py`, `tests/unit/setup_ddl/test_extraction.py`

Responsibilities:

- `assembly.py`: DDL file assembly from staging JSON.
- `discovery.py`: source database/schema discovery.
- `db_extraction.py`: technology dispatch to SQL Server or Oracle extraction backends.
- `extract_orchestration.py`: `run_extract` orchestration and catalog path tracking.
- `extract.py`: compatibility facade that re-exports existing public functions.

### Task 2: Manifest Boundary Split

- Create: `lib/shared/setup_ddl_support/manifest_io.py`
- Create: `lib/shared/setup_ddl_support/runtime_identity.py`
- Create: `lib/shared/setup_ddl_support/oracle_schema_summary.py`
- Modify: `lib/shared/setup_ddl_support/manifest.py`
- Test: `tests/unit/setup_ddl/test_manifest_boundaries.py`
- Existing tests to run: `tests/unit/setup_ddl/test_manifest_and_handoff.py`, `tests/unit/setup_ddl/test_oracle_extract.py`

Responsibilities:

- `manifest_io.py`: manifest reads, partial/full manifest writes, and init handoff reads.
- `runtime_identity.py`: runtime-role construction and source identity comparison.
- `oracle_schema_summary.py`: Oracle schema object-count summary.
- `manifest.py`: compatibility facade that re-exports existing public functions, constants, and exception types.

### Task 3: Integration and Verification

- Modify: `repo-map.json` if the setup-ddl module descriptions become stale.
- Run focused setup-ddl tests.
- Run import compatibility checks for public facades.
- Commit integration metadata if needed.

## Task 1: Split Extraction Support Modules

**Files:**

- Create: `lib/shared/setup_ddl_support/assembly.py`
- Create: `lib/shared/setup_ddl_support/discovery.py`
- Create: `lib/shared/setup_ddl_support/db_extraction.py`
- Create: `lib/shared/setup_ddl_support/extract_orchestration.py`
- Modify: `lib/shared/setup_ddl_support/extract.py`
- Test: `tests/unit/setup_ddl/test_extract_boundaries.py`

- [ ] **Step 1: Write failing import-boundary tests**

Create `tests/unit/setup_ddl/test_extract_boundaries.py`:

```python
"""Import-boundary tests for split setup-ddl extraction support modules."""

from __future__ import annotations


def test_extract_facade_reexports_existing_public_entrypoints() -> None:
    from shared.setup_ddl_support import extract

    assert callable(extract.run_assemble_modules)
    assert callable(extract.run_assemble_tables)
    assert callable(extract.assemble_ddl_from_staging)
    assert callable(extract.run_list_databases)
    assert callable(extract.run_list_schemas)
    assert callable(extract.run_db_extraction)
    assert callable(extract.run_extract)


def test_extraction_support_modules_own_split_entrypoints() -> None:
    from shared.setup_ddl_support.assembly import (
        assemble_ddl_from_staging,
        run_assemble_modules,
        run_assemble_tables,
    )
    from shared.setup_ddl_support.db_extraction import run_db_extraction
    from shared.setup_ddl_support.discovery import run_list_databases, run_list_schemas
    from shared.setup_ddl_support.extract_orchestration import run_extract

    assert callable(run_assemble_modules)
    assert callable(run_assemble_tables)
    assert callable(assemble_ddl_from_staging)
    assert callable(run_list_databases)
    assert callable(run_list_schemas)
    assert callable(run_db_extraction)
    assert callable(run_extract)
```

- [ ] **Step 2: Run the new boundary tests and verify they fail**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_extract_boundaries.py -q
```

Expected: FAIL with `ModuleNotFoundError` for the new support modules.

- [ ] **Step 3: Move assembly functions into `assembly.py`**

Move these functions and their direct helper dependencies from `extract.py` into `assembly.py`:

```text
run_assemble_modules
run_assemble_tables
_repo_relative
assemble_ddl_from_staging
```

`assembly.py` must import only what it uses:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.name_resolver import normalize
from shared.setup_ddl_support.manifest import require_technology
from shared.setup_ddl_support.staging_io import read_json, read_json_optional
from shared.sql_types import format_sql_type
```

Keep behavior identical:

```python
def run_assemble_modules(input_path: Path, project_root: Path, object_type: str) -> dict[str, Any]:
    if object_type not in ("procedures", "views", "functions"):
        raise ValueError(f"Invalid type: {object_type}. Must be procedures, views, or functions.")
    rows = read_json(input_path)
    blocks = [row.get("definition", "").strip() for row in rows if row.get("definition")]
    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / f"{object_type}.sql"
    out_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return {"file": str(out_path), "count": len(blocks)}
```

- [ ] **Step 4: Move discovery functions into `discovery.py`**

Move these functions from `extract.py` into `discovery.py`:

```text
run_list_databases
run_list_schemas
```

`discovery.py` must import the Oracle summary from the split manifest support path through the public facade:

```python
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts, oracle_connect, sql_server_connect
from shared.setup_ddl_support.manifest import (
    UnsupportedOperationError,
    build_oracle_schema_summary,
    require_technology,
)

logger = logging.getLogger(__name__)
```

Preserve the current SQL Server and Oracle query strings exactly unless a test requires an intentional change.

- [ ] **Step 5: Move extraction dispatch into `db_extraction.py`**

Create `db_extraction.py` with the existing dispatch behavior:

```python
"""Technology dispatch for setup-ddl source extraction."""

from __future__ import annotations

from pathlib import Path


def run_db_extraction(technology: str, staging_dir: Path, db_name: str, schemas: list[str]) -> None:
    if technology == "sql_server":
        from shared.sqlserver_extract import run_sqlserver_extraction

        run_sqlserver_extraction(staging_dir, db_name, schemas)
    elif technology == "oracle":
        from shared.oracle_extract import run_oracle_extraction

        run_oracle_extraction(staging_dir, schemas)
    else:
        raise ValueError(f"setup-ddl extract is not supported for technology '{technology}'")
```

- [ ] **Step 6: Move orchestration into `extract_orchestration.py`**

Move these functions and helpers from `extract.py` into `extract_orchestration.py`:

```text
_catalog_snapshot
_changed_catalog_paths
run_extract
```

`extract_orchestration.py` should import split functions from their owning modules:

```python
from shared.setup_ddl_support.assembly import assemble_ddl_from_staging
from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale, run_write_catalog
from shared.setup_ddl_support.db_extraction import run_db_extraction
from shared.setup_ddl_support.manifest import (
    TECH_DIALECT,
    get_connection_identity,
    identity_changed,
    read_manifest_strict,
    require_technology,
    run_write_manifest,
)
```

Keep lazy imports inside `run_extract` for catalog preservation, enrichment, and diagnostics:

```python
def run_extract(project_root: Path, database: str | None, schemas: list[str]) -> dict[str, Any]:
    from shared.catalog import restore_enriched_fields, snapshot_enriched_fields
    from shared.catalog_enrich import enrich_catalog
    from shared.diagnostics import run_diagnostics
```

- [ ] **Step 7: Replace `extract.py` with a compatibility facade**

After moving implementation, `extract.py` should be import-only:

```python
"""Compatibility facade for setup-ddl extraction and source discovery."""

from __future__ import annotations

from shared.setup_ddl_support.assembly import (
    assemble_ddl_from_staging,
    run_assemble_modules,
    run_assemble_tables,
)
from shared.setup_ddl_support.db_extraction import run_db_extraction
from shared.setup_ddl_support.discovery import run_list_databases, run_list_schemas
from shared.setup_ddl_support.extract_orchestration import run_extract

__all__ = [
    "assemble_ddl_from_staging",
    "run_assemble_modules",
    "run_assemble_tables",
    "run_db_extraction",
    "run_extract",
    "run_list_databases",
    "run_list_schemas",
]
```

- [ ] **Step 8: Preserve monkeypatch compatibility for existing tests**

Existing tests patch attributes on `shared.setup_ddl_support.extract`. If those tests fail because `run_extract` now uses symbols imported into `extract_orchestration.py`, update the tests to patch the owning module directly.

For example, replace this pattern:

```python
patch.object(setup_ddl_extract, "run_db_extraction")
```

with:

```python
from shared.setup_ddl_support import extract_orchestration

patch.object(extract_orchestration, "run_db_extraction")
```

Do not add delegation back into `extract.py` just to support monkeypatching private implementation details.

- [ ] **Step 9: Run focused extraction tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/setup_ddl/test_extract_boundaries.py \
  ../tests/unit/setup_ddl/test_assembly.py \
  ../tests/unit/setup_ddl/test_extraction.py -q
```

Expected: PASS.

- [ ] **Step 10: Commit Task 1**

Run:

```bash
git add \
  lib/shared/setup_ddl_support/assembly.py \
  lib/shared/setup_ddl_support/discovery.py \
  lib/shared/setup_ddl_support/db_extraction.py \
  lib/shared/setup_ddl_support/extract_orchestration.py \
  lib/shared/setup_ddl_support/extract.py \
  tests/unit/setup_ddl/test_extract_boundaries.py \
  tests/unit/setup_ddl/test_extraction.py
git commit -m "refactor: split setup ddl extraction support"
```

Only include `tests/unit/setup_ddl/test_extraction.py` if Step 8 required updates.

## Task 2: Split Manifest Support Modules

**Files:**

- Create: `lib/shared/setup_ddl_support/manifest_io.py`
- Create: `lib/shared/setup_ddl_support/runtime_identity.py`
- Create: `lib/shared/setup_ddl_support/oracle_schema_summary.py`
- Modify: `lib/shared/setup_ddl_support/manifest.py`
- Test: `tests/unit/setup_ddl/test_manifest_boundaries.py`

- [ ] **Step 1: Write failing import-boundary tests**

Create `tests/unit/setup_ddl/test_manifest_boundaries.py`:

```python
"""Import-boundary tests for split setup-ddl manifest support modules."""

from __future__ import annotations


def test_manifest_facade_reexports_existing_public_entrypoints() -> None:
    from shared.setup_ddl_support import manifest

    assert callable(manifest.require_technology)
    assert callable(manifest.read_manifest_strict)
    assert callable(manifest.read_manifest_or_empty)
    assert callable(manifest.run_write_partial_manifest)
    assert callable(manifest.run_read_handoff)
    assert callable(manifest.run_write_manifest)
    assert callable(manifest.get_connection_identity)
    assert callable(manifest.identity_changed)
    assert callable(manifest.build_runtime_role)
    assert callable(manifest.build_oracle_schema_summary)
    assert isinstance(manifest.TECH_DIALECT, dict)


def test_manifest_support_modules_own_split_entrypoints() -> None:
    from shared.setup_ddl_support.manifest_io import (
        read_manifest_or_empty,
        read_manifest_strict,
        require_technology,
        run_read_handoff,
        run_write_manifest,
        run_write_partial_manifest,
    )
    from shared.setup_ddl_support.oracle_schema_summary import build_oracle_schema_summary
    from shared.setup_ddl_support.runtime_identity import (
        build_runtime_role,
        get_connection_identity,
        identity_changed,
    )

    assert callable(require_technology)
    assert callable(read_manifest_strict)
    assert callable(read_manifest_or_empty)
    assert callable(run_write_partial_manifest)
    assert callable(run_read_handoff)
    assert callable(run_write_manifest)
    assert callable(build_oracle_schema_summary)
    assert callable(get_connection_identity)
    assert callable(identity_changed)
    assert callable(build_runtime_role)
```

- [ ] **Step 2: Run the new boundary tests and verify they fail**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_manifest_boundaries.py -q
```

Expected: FAIL with `ModuleNotFoundError` for the new support modules.

- [ ] **Step 3: Move Oracle schema summary into `oracle_schema_summary.py`**

Create `oracle_schema_summary.py`:

```python
"""Oracle source schema summary helpers for setup-ddl discovery."""

from __future__ import annotations

from typing import Any


def build_oracle_schema_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entry_type = dict[str, Any]
    buckets: dict[str, entry_type] = {}
    for row in rows:
        owner = row.get("OWNER") or row.get("owner") or ""
        obj_type = (row.get("OBJECT_TYPE") or row.get("object_type") or "").upper()
        if not owner:
            continue
        if owner not in buckets:
            buckets[owner] = {
                "owner": owner,
                "tables": 0,
                "procedures": 0,
                "views": 0,
                "functions": 0,
                "materialized_views": 0,
            }
        if obj_type == "TABLE":
            buckets[owner]["tables"] += 1
        elif obj_type == "PROCEDURE":
            buckets[owner]["procedures"] += 1
        elif obj_type == "VIEW":
            buckets[owner]["views"] += 1
        elif obj_type == "MATERIALIZED VIEW":
            buckets[owner]["materialized_views"] += 1
        elif obj_type == "FUNCTION":
            buckets[owner]["functions"] += 1
    return sorted(buckets.values(), key=lambda x: x["owner"])
```

- [ ] **Step 4: Move runtime identity into `runtime_identity.py`**

Move these functions from `manifest.py` into `runtime_identity.py`:

```text
get_connection_identity
identity_changed
build_runtime_role
```

`runtime_identity.py` must import:

```python
from __future__ import annotations

import os
from typing import Any

from shared.runtime_config import TECH_DIALECT, dialect_for_technology, get_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
```

Keep the current `identity_changed` behavior exactly, including the `schema` to `schema_name` field mapping.

- [ ] **Step 5: Move manifest IO and write helpers into `manifest_io.py`**

Move these functions and helper from `manifest.py` into `manifest_io.py`:

```text
require_technology
read_manifest_strict
read_manifest_or_empty
_seed_runtime_role
run_write_partial_manifest
run_read_handoff
run_write_manifest
```

`manifest_io.py` must import runtime identity from the new support module:

```python
from shared.setup_ddl_support.runtime_identity import build_runtime_role
```

It must preserve warning behavior for unreadable or invalid optional manifests:

```python
logger.warning("event=manifest_read_error path=%s error=%s", manifest_path, exc)
```

It must preserve handoff read warning behavior:

```python
logger.warning("event=read_handoff_error operation=read_manifest error=%s", exc)
```

- [ ] **Step 6: Replace `manifest.py` with a compatibility facade**

After moving implementation, `manifest.py` should re-export the same public surface:

```python
"""Compatibility facade for setup-ddl manifest and source-identity helpers."""

from __future__ import annotations

from shared.runtime_config import TECH_DIALECT
from shared.setup_ddl_support.manifest_io import (
    UnsupportedOperationError,
    read_manifest_or_empty,
    read_manifest_strict,
    require_technology,
    run_read_handoff,
    run_write_manifest,
    run_write_partial_manifest,
)
from shared.setup_ddl_support.oracle_schema_summary import build_oracle_schema_summary
from shared.setup_ddl_support.runtime_identity import (
    build_runtime_role,
    get_connection_identity,
    identity_changed,
)

__all__ = [
    "TECH_DIALECT",
    "UnsupportedOperationError",
    "build_oracle_schema_summary",
    "build_runtime_role",
    "get_connection_identity",
    "identity_changed",
    "read_manifest_or_empty",
    "read_manifest_strict",
    "require_technology",
    "run_read_handoff",
    "run_write_manifest",
    "run_write_partial_manifest",
]
```

Define `UnsupportedOperationError` in `manifest_io.py` so existing imports from `manifest.py` still receive the same exception class through the facade.

- [ ] **Step 7: Run focused manifest tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/setup_ddl/test_manifest_boundaries.py \
  ../tests/unit/setup_ddl/test_manifest_and_handoff.py \
  ../tests/unit/setup_ddl/test_oracle_extract.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 2**

Run:

```bash
git add \
  lib/shared/setup_ddl_support/manifest_io.py \
  lib/shared/setup_ddl_support/runtime_identity.py \
  lib/shared/setup_ddl_support/oracle_schema_summary.py \
  lib/shared/setup_ddl_support/manifest.py \
  tests/unit/setup_ddl/test_manifest_boundaries.py
git commit -m "refactor: split setup ddl manifest support"
```

## Task 3: Integration Metadata and Verification

**Files:**

- Modify: `repo-map.json` if stale.
- No production code changes unless needed to resolve integration issues from Tasks 1 and 2.

- [ ] **Step 1: Check current setup-ddl module inventory**

Run:

```bash
rg "setup_ddl_support/" repo-map.json
rg "setup_ddl_support" repo-map.json
```

If `repo-map.json` still describes `extract.py` or `manifest.py` as owning behavior now moved into support modules, update the relevant module description. Do not add volatile counts or implementation details.

- [ ] **Step 2: Run public import compatibility checks**

Run:

```bash
cd lib && uv run python - <<'PY'
from shared.setup_ddl_support.extract import (
    assemble_ddl_from_staging,
    run_assemble_modules,
    run_assemble_tables,
    run_db_extraction,
    run_extract,
    run_list_databases,
    run_list_schemas,
)
from shared.setup_ddl_support.manifest import (
    TECH_DIALECT,
    UnsupportedOperationError,
    build_oracle_schema_summary,
    build_runtime_role,
    get_connection_identity,
    identity_changed,
    read_manifest_or_empty,
    read_manifest_strict,
    require_technology,
    run_read_handoff,
    run_write_manifest,
    run_write_partial_manifest,
)

print("extract imports ok", run_extract, run_list_schemas, assemble_ddl_from_staging)
print("manifest imports ok", TECH_DIALECT, UnsupportedOperationError, get_connection_identity)
PY
```

Expected: exits `0` and prints both `imports ok` lines.

- [ ] **Step 3: Run focused setup-ddl verification**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl -q
```

Expected: PASS.

- [ ] **Step 4: Run CLI smoke checks**

Run:

```bash
cd packages/ad-migration-internal && uv run setup-ddl --help >/tmp/setup-ddl-help.txt
cd packages/ad-migration-cli && uv run ad-migration setup-source --help >/tmp/setup-source-help.txt
```

Expected: both commands exit `0`.

- [ ] **Step 5: Commit integration metadata if needed**

If `repo-map.json` changed, run:

```bash
git add repo-map.json
git commit -m "docs: update setup ddl module map"
```

If no integration metadata changed, do not create an empty commit.

- [ ] **Step 6: Final review handoff**

Request a final code review subagent with this scope:

```text
Review /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup for setup-ddl boundary cleanup.
Focus on:
- public import compatibility for shared.setup_ddl_support.extract and manifest
- whether moved modules have clear responsibilities
- behavior regressions in setup-ddl assembly, discovery, extraction orchestration, manifest writes, identity comparison, and Oracle schema summary
- tests that patch the wrong module or overfit implementation details
Report findings first with file:line references.
```
