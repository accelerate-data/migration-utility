# Modular Source Restructure Pass 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Continue reducing >300-line source modules by splitting clear implementation seams into focused support modules while preserving public import paths and CLI behavior.

**Architecture:** Keep existing public facades stable (`shared.profile`, `shared.catalog`, `shared.diagnostics.common`, `shared.loader_io`). Move implementation-only functions into adjacent support packages with narrow responsibilities, then re-export from the original modules. Split tests only where they directly track the new ownership boundaries.

**Tech Stack:** Python 3.11, Typer, Pydantic v2, pytest, uv, repo-local `repo-map.json`.

---

## Current Worktree

Worktree: `/Users/hbanerjee/src/worktrees/modular-source-restructure-pass-2`

Branch: `modular-source-restructure-pass-2`

The branch was created from current `main` after PR #349 merged. The starting source-only >300-line candidates are:

```text
586 lib/shared/catalog_models.py
463 lib/shared/oracle_extract.py
451 lib/shared/profile.py
446 lib/shared/catalog.py
441 lib/shared/diagnostics/common.py
437 lib/shared/loader_parse.py
397 lib/shared/sqlserver_extract.py
374 lib/shared/catalog_enrich.py
364 lib/shared/init_templates.py
361 lib/shared/loader_io.py
351 mcp/ddl/ddl_mcp_support/loader_parse.py
321 lib/shared/output_models/dry_run.py
314 lib/shared/migrate_support/artifacts.py
303 lib/shared/discover_support/browse.py
302 lib/shared/setup_ddl_support/staging_signals.py
```

## Parallel Workstreams

These tasks can run in parallel because their write sets are disjoint:

- Task 1: `shared.profile` split.
- Task 2: `shared.catalog` split.
- Task 3: `shared.diagnostics.common` split.
- Task 4: `shared.loader_io` split.

Run Task 5 only after Tasks 1-4 are merged into the integration worktree.

Subagent ownership rules:

- Each subagent owns only the files listed in its assigned task's **Files** section.
- Do not edit `repo-map.json` in Tasks 1-4; Task 5 owns integration metadata.
- Do not refactor adjacent >300-line files outside the assigned task, even if they look related.
- If a task needs a file outside its listed write set, stop and report the required file before editing it.
- The integration agent owns final inventory, cross-task conflict resolution, full verification, and PR preparation.

## File Structure

### Task 1: Profile Support

- Create: `lib/shared/profile_support/__init__.py`
- Create: `lib/shared/profile_support/seed.py`
- Create: `lib/shared/profile_support/table_context.py`
- Create: `lib/shared/profile_support/view_context.py`
- Create: `lib/shared/profile_support/writeback.py`
- Modify: `lib/shared/profile.py`
- Test: `tests/unit/profile/test_table_context.py`
- Test: `tests/unit/profile/test_view_context.py`
- Test: `tests/unit/profile/test_table_write.py`
- Test: `tests/unit/profile/test_view_write.py`

Responsibility split:

- `seed.py`: seed profile payload construction.
- `table_context.py`: table profiling context assembly and related procedure loading.
- `view_context.py`: view profiling context assembly and enriched reference lists.
- `writeback.py`: status derivation and table/view profile catalog writes.
- `profile.py`: Typer CLI only plus compatibility exports.

### Task 2: Catalog Support

- Create: `lib/shared/catalog_support/__init__.py`
- Create: `lib/shared/catalog_support/paths.py`
- Create: `lib/shared/catalog_support/loaders.py`
- Create: `lib/shared/catalog_support/merge.py`
- Create: `lib/shared/catalog_support/references.py`
- Create: `lib/shared/catalog_support/writers.py`
- Modify: `lib/shared/catalog.py`
- Test: `tests/unit/catalog/test_catalog.py`
- Test: `tests/unit/catalog/test_catalog_preservation_helpers.py`

Responsibility split:

- `paths.py`: catalog directory, object path, bucket/type detection, path resolution.
- `loaders.py`: `_load_catalog_file`, typed catalog loaders, selected-writer read.
- `merge.py`: atomic JSON write and `load_and_merge_catalog`.
- `references.py`: `ensure_references` and `ensure_referenced_by`.
- `writers.py`: table/procedure/view/function catalog writers and procedure statement/slice helpers.
- `catalog.py`: compatibility barrel that re-exports public functions.

### Task 3: Common Diagnostics Support

- Create: `lib/shared/diagnostics/common_support/__init__.py`
- Create: `lib/shared/diagnostics/common_support/object_checks.py`
- Create: `lib/shared/diagnostics/common_support/reference_checks.py`
- Create: `lib/shared/diagnostics/common_support/dependency_checks.py`
- Create: `lib/shared/diagnostics/common_support/graph.py`
- Modify: `lib/shared/diagnostics/common.py`
- Test: `tests/unit/diagnostics/test_diagnostics.py`

Responsibility split:

- `object_checks.py`: parse error, unsupported syntax, stale object, multi-table read/write.
- `reference_checks.py`: missing, out-of-scope, remote EXEC checks.
- `graph.py`: catalog JSON loading, dependency extraction, traversal helpers.
- `dependency_checks.py`: circular reference, dependency error, transitive scope leak, nested view chain.
- `common.py`: import-only registration facade.

### Task 4: Loader I/O Support

- Create: `lib/shared/loader_io_support/__init__.py`
- Create: `lib/shared/loader_io_support/manifest.py`
- Create: `lib/shared/loader_io_support/directory.py`
- Create: `lib/shared/loader_io_support/indexing.py`
- Create: `lib/shared/loader_io_support/load.py`
- Modify: `lib/shared/loader_io.py`
- Test: `tests/unit/loader_io/test_loader_io.py`
- Test: `tests/unit/loader_parse/test_loader_parse.py`

Responsibility split:

- `manifest.py`: manifest read/write/clear sandbox helpers.
- `directory.py`: SQL file loading, delimiter map, DDL directory loading.
- `indexing.py`: per-object file writing, catalog indexing, catalog loading.
- `load.py`: top-level DDL loading orchestration.
- `loader_io.py`: compatibility barrel.

### Task 5: Integration Metadata and Verification

- Modify: `repo-map.json`
- Run source inventory again.
- Run focused tests for each split.
- Run repo-level Python verification.
- Commit each workstream independently, then an integration metadata commit if needed.

---

### Task 1: Split `shared.profile`

**Files:**

- Create: `lib/shared/profile_support/__init__.py`
- Create: `lib/shared/profile_support/seed.py`
- Create: `lib/shared/profile_support/table_context.py`
- Create: `lib/shared/profile_support/view_context.py`
- Create: `lib/shared/profile_support/writeback.py`
- Modify: `lib/shared/profile.py`
- Test: `tests/unit/profile/test_table_context.py`
- Test: `tests/unit/profile/test_view_context.py`
- Test: `tests/unit/profile/test_table_write.py`
- Test: `tests/unit/profile/test_view_write.py`

- [ ] **Step 1: Write import-boundary tests for the new support modules**

Append these tests to `tests/unit/profile/test_table_context.py`:

```python
def test_profile_support_exports_table_context() -> None:
    from shared.profile_support.table_context import run_context

    result = run_context(
        _PROFILE_FIXTURES,
        "silver.FactSales",
        "dbo.usp_load_fact_sales",
    )

    assert result.table == "silver.factsales"
    assert result.writer == "dbo.usp_load_fact_sales"
```

Append this test to `tests/unit/profile/test_view_context.py`:

```python
def test_profile_support_exports_view_context() -> None:
    from shared.profile_support.view_context import run_view_context

    result = run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")

    assert result.view == "silver.vw_multi"
    assert result.references.tables.in_scope
```

Append this test to `tests/unit/profile/test_table_write.py`:

```python
def test_profile_support_exports_write_helpers() -> None:
    from shared.profile_support.writeback import derive_table_profile_status

    section = TableProfileSection.model_validate({
        "classification": {
            "resolved_kind": "dimension",
            "source": "llm",
            "rationale": "Dimension table.",
        },
        "primary_key": {"columns": ["id"], "source": "catalog"},
    })

    assert derive_table_profile_status(section) == "ok"
```

- [ ] **Step 2: Run the new tests and verify they fail before support modules exist**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/profile/test_table_context.py::test_profile_support_exports_table_context \
  ../tests/unit/profile/test_view_context.py::test_profile_support_exports_view_context \
  ../tests/unit/profile/test_table_write.py::test_profile_support_exports_write_helpers -q
```

Expected: fail with `ModuleNotFoundError: No module named 'shared.profile_support'`.

- [ ] **Step 3: Create the support package exports**

Create `lib/shared/profile_support/__init__.py`:

```python
"""Support modules for shared.profile."""

from shared.profile_support.seed import build_seed_profile
from shared.profile_support.table_context import run_context
from shared.profile_support.view_context import run_view_context
from shared.profile_support.writeback import (
    derive_table_profile_status,
    derive_view_profile_status,
    run_write,
)

__all__ = [
    "build_seed_profile",
    "derive_table_profile_status",
    "derive_view_profile_status",
    "run_context",
    "run_view_context",
    "run_write",
]
```

- [ ] **Step 4: Move seed profile construction**

Create `lib/shared/profile_support/seed.py`:

```python
"""Seed profile helpers."""

from __future__ import annotations

from typing import Any


def build_seed_profile(rationale: str = "Table is maintained as a dbt seed.") -> dict[str, Any]:
    """Build the canonical profile payload for a dbt seed table."""
    return {
        "classification": {
            "resolved_kind": "seed",
            "source": "catalog",
            "rationale": rationale,
        },
        "warnings": [],
        "errors": [],
    }
```

Remove `build_seed_profile` from `lib/shared/profile.py` and import it from `shared.profile_support`.

- [ ] **Step 5: Move table context code unchanged**

Create `lib/shared/profile_support/table_context.py`.

Move these exact complete function definitions from `lib/shared/profile.py` into the new file:

- `_extract_catalog_signals`
- `_build_related_procedures`
- `run_context`

Use this import block in the new file:

```python
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_proc_catalog, load_table_catalog, read_selected_writer
from shared.catalog_models import ReferencesBucket, TableCatalog
from shared.context_helpers import (
    project_sql_dialect,
    references_from_selected_sql,
    resolve_selected_writer_ddl_slice,
    target_visible_columns,
)
from shared.loader import CatalogFileMissingError, load_ddl
from shared.name_resolver import normalize
from shared.output_models.profile import (
    CatalogSignals,
    ProfileColumnDef,
    ProfileContext,
    RelatedProcedure,
)

logger = logging.getLogger(__name__)
```

Do not change function behavior.

- [ ] **Step 6: Move view context code unchanged**

Create `lib/shared/profile_support/view_context.py`.

Move these exact complete function definitions from `lib/shared/profile.py` into the new file:

- `_build_enriched_ref_list`
- `run_view_context`

Use this import block in the new file:

```python
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_view_catalog
from shared.context_helpers import target_visible_columns
from shared.loader import CatalogFileMissingError
from shared.name_resolver import normalize
from shared.output_models.discover import SqlElement
from shared.output_models.profile import (
    EnrichedInScopeRef,
    EnrichedScopedRefList,
    OutOfScopeRef,
    ViewColumnDef,
    ViewProfileContext,
    ViewReferencedBy,
    ViewReferences,
)

logger = logging.getLogger(__name__)
```

Do not change function behavior.

- [ ] **Step 7: Move writeback code unchanged**

Create `lib/shared/profile_support/writeback.py`.

Move these exact complete function definitions from `lib/shared/profile.py` into the new file:

- `derive_table_profile_status`
- `derive_view_profile_status`
- `_profile_payload_with_status`
- `_write_view_profile`
- `run_write`

Use this import block:

```python
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import (
    load_and_merge_catalog,
    load_table_catalog,
    write_json as _write_catalog_json,
)
from shared.catalog_models import TableProfileSection, ViewProfileSection
from shared.env_config import resolve_catalog_dir
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)
```

Do not change function behavior.

- [ ] **Step 8: Reduce `lib/shared/profile.py` to CLI facade plus exports**

Replace implementation imports in `lib/shared/profile.py` with:

```python
from shared.profile_support import (
    build_seed_profile,
    derive_table_profile_status,
    derive_view_profile_status,
    run_context,
    run_view_context,
    run_write,
)
```

Keep the existing Typer `app`, `context`, `view_context`, and `write` commands in `profile.py`. Keep exception handling and `emit()` behavior unchanged.

- [ ] **Step 9: Run focused profile tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/profile -q
```

Expected: all profile tests pass.

- [ ] **Step 10: Commit Task 1**

Run:

```bash
git add \
  lib/shared/profile.py \
  lib/shared/profile_support/__init__.py \
  lib/shared/profile_support/seed.py \
  lib/shared/profile_support/table_context.py \
  lib/shared/profile_support/view_context.py \
  lib/shared/profile_support/writeback.py \
  tests/unit/profile/test_table_context.py \
  tests/unit/profile/test_view_context.py \
  tests/unit/profile/test_table_write.py
git commit -m "refactor: split profile support modules"
```

---

### Task 2: Split `shared.catalog`

**Files:**

- Create: `lib/shared/catalog_support/__init__.py`
- Create: `lib/shared/catalog_support/paths.py`
- Create: `lib/shared/catalog_support/loaders.py`
- Create: `lib/shared/catalog_support/merge.py`
- Create: `lib/shared/catalog_support/references.py`
- Create: `lib/shared/catalog_support/writers.py`
- Modify: `lib/shared/catalog.py`
- Test: `tests/unit/catalog/test_catalog.py`

- [ ] **Step 1: Write import-boundary tests**

Append to `tests/unit/catalog/test_catalog.py`:

```python
def test_catalog_support_exports_core_helpers(tmp_path: Path) -> None:
    from shared.catalog_support.paths import detect_catalog_bucket
    from shared.catalog_support.references import ensure_references

    catalog_dir = tmp_path / "catalog" / "tables"
    catalog_dir.mkdir(parents=True)
    (catalog_dir / "silver.dimcustomer.json").write_text("{}", encoding="utf-8")

    assert detect_catalog_bucket(tmp_path, "silver.DimCustomer") == "tables"
    assert "tables" in ensure_references({})["references"]
```

- [ ] **Step 2: Run the new test and verify it fails before support modules exist**

Run:

```bash
cd lib && uv run pytest ../tests/unit/catalog/test_catalog.py::test_catalog_support_exports_core_helpers -q
```

Expected: fail with `ModuleNotFoundError: No module named 'shared.catalog_support'`.

- [ ] **Step 3: Create catalog support exports**

Create `lib/shared/catalog_support/__init__.py`:

```python
"""Support modules for shared.catalog."""
```

- [ ] **Step 4: Move path and detection helpers**

Create `lib/shared/catalog_support/paths.py`.

Move these exact complete function definitions from `lib/shared/catalog.py`:

- `_catalog_dir`
- `_object_path`
- `has_catalog`
- `resolve_catalog_path`
- `detect_catalog_bucket`
- `detect_object_type`

Use this import block:

```python
from __future__ import annotations

import json
from pathlib import Path

from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogFileMissingError
from shared.name_resolver import normalize
```

For `detect_object_type`, read the view catalog JSON directly when checking
`is_materialized_view`. Do not import `load_view_catalog`; that would create a
`paths` ↔ `loaders` cycle.

- [ ] **Step 5: Move typed catalog loaders**

Create `lib/shared/catalog_support/loaders.py`.

Move these exact complete function definitions from `lib/shared/catalog.py`:

- `_load_catalog_file`
- `load_table_catalog`
- `load_proc_catalog`
- `load_view_catalog`
- `load_function_catalog`
- `read_selected_writer`

Use this import block:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.catalog_models import FunctionCatalog, ProcedureCatalog, TableCatalog, ViewCatalog
from shared.catalog_support.paths import _object_path
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
```

- [ ] **Step 6: Move merge helpers**

Create `lib/shared/catalog_support/merge.py`.

Move these exact complete function definitions from `lib/shared/catalog.py`:

- `write_json`
- `load_and_merge_catalog`

Use this import block:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.catalog_support.paths import resolve_catalog_path
from shared.loader_data import CatalogLoadError
```

- [ ] **Step 7: Move reference normalization helpers**

Create `lib/shared/catalog_support/references.py`.

Move these exact complete function definitions from `lib/shared/catalog.py`:

- `ensure_references`
- `ensure_referenced_by`

Use this import block:

```python
from __future__ import annotations

from typing import Any

from shared.dmf_processing import empty_scoped
```

- [ ] **Step 8: Move writer helpers**

Create `lib/shared/catalog_support/writers.py`.

Move these exact complete function definitions from `lib/shared/catalog.py`:

- `write_table_catalog`
- `write_proc_statements`
- `write_proc_table_slice`
- `_write_catalog_json`
- `write_proc_catalog`
- `write_view_catalog`
- `write_function_catalog`

Use imports for the helpers moved in earlier steps:

```python
from shared.catalog_support.merge import write_json
from shared.catalog_support.paths import _object_path
from shared.catalog_support.references import ensure_referenced_by, ensure_references
from shared.name_resolver import fqn_parts, normalize
```

Do not change the payload shape or write paths.

- [ ] **Step 9: Convert `lib/shared/catalog.py` to compatibility barrel**

Replace moved function bodies in `lib/shared/catalog.py` with imports:

```python
from shared.catalog_preservation import (
    restore_enriched_fields as restore_enriched_fields,
    snapshot_enriched_fields as snapshot_enriched_fields,
)
from shared.catalog_support.loaders import (
    load_function_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
)
from shared.catalog_support.merge import load_and_merge_catalog, write_json
from shared.catalog_support.paths import (
    _catalog_dir,
    _object_path,
    detect_catalog_bucket,
    detect_object_type,
    has_catalog,
    resolve_catalog_path,
)
from shared.catalog_support.references import ensure_referenced_by, ensure_references
from shared.catalog_support.writers import (
    _write_catalog_json,
    write_function_catalog,
    write_proc_catalog,
    write_proc_statements,
    write_proc_table_slice,
    write_table_catalog,
    write_view_catalog,
)
```

- [ ] **Step 10: Run focused catalog tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/catalog -q
```

Expected: all catalog tests pass.

- [ ] **Step 11: Commit Task 2**

Run:

```bash
git add \
  lib/shared/catalog.py \
  lib/shared/catalog_support/__init__.py \
  lib/shared/catalog_support/paths.py \
  lib/shared/catalog_support/loaders.py \
  lib/shared/catalog_support/merge.py \
  lib/shared/catalog_support/references.py \
  lib/shared/catalog_support/writers.py \
  tests/unit/catalog/test_catalog.py
git commit -m "refactor: split catalog support modules"
```

---

### Task 3: Split `shared.diagnostics.common`

**Files:**

- Create: `lib/shared/diagnostics/common_support/__init__.py`
- Create: `lib/shared/diagnostics/common_support/object_checks.py`
- Create: `lib/shared/diagnostics/common_support/reference_checks.py`
- Create: `lib/shared/diagnostics/common_support/graph.py`
- Create: `lib/shared/diagnostics/common_support/dependency_checks.py`
- Modify: `lib/shared/diagnostics/common.py`
- Test: `tests/unit/diagnostics/test_diagnostics.py`

- [ ] **Step 1: Write import-boundary tests**

Append to `tests/unit/diagnostics/test_diagnostics.py`:

```python
def test_common_support_modules_import_checks() -> None:
    from shared.diagnostics.common_support.object_checks import check_parse_error
    from shared.diagnostics.common_support.reference_checks import check_missing_reference
    from shared.diagnostics.common_support.dependency_checks import check_circular_reference

    assert check_parse_error.__name__ == "check_parse_error"
    assert check_missing_reference.__name__ == "check_missing_reference"
    assert check_circular_reference.__name__ == "check_circular_reference"
```

- [ ] **Step 2: Run the new test and verify it fails before support modules exist**

Run:

```bash
cd lib && uv run pytest ../tests/unit/diagnostics/test_diagnostics.py::test_common_support_modules_import_checks -q
```

Expected: fail with `ModuleNotFoundError: No module named 'shared.diagnostics.common_support'`.

- [ ] **Step 3: Create support package**

Create `lib/shared/diagnostics/common_support/__init__.py`:

```python
"""Cross-dialect diagnostic check support modules."""
```

- [ ] **Step 4: Move object-level checks**

Create `lib/shared/diagnostics/common_support/object_checks.py`.

Move these exact complete function definitions from `lib/shared/diagnostics/common.py`:

- `_has_llm_recovery_statements`
- `check_parse_error`
- `check_unsupported_syntax`
- `check_stale_object`
- `check_multi_table_write`
- `check_multi_table_read`

Use this import block:

```python
from __future__ import annotations

import logging
from typing import Any

from shared.diagnostics import CatalogContext, DiagnosticResult, diagnostic

logger = logging.getLogger(__name__)
```

- [ ] **Step 5: Move reference checks**

Create `lib/shared/diagnostics/common_support/reference_checks.py`.

Move these exact complete function definitions from `lib/shared/diagnostics/common.py`:

- `check_missing_reference`
- `check_out_of_scope_reference`
- `check_remote_exec_unsupported`

Use this import block:

```python
from __future__ import annotations

from shared.diagnostics import CatalogContext, DiagnosticResult, diagnostic
```

- [ ] **Step 6: Move graph helpers**

Create `lib/shared/diagnostics/common_support/graph.py`.

Move these exact complete helper definitions from `lib/shared/diagnostics/common.py`:

- `_load_catalog_json`
- `_get_dep_fqns`

Use this import block:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
```

- [ ] **Step 7: Move dependency graph checks**

Create `lib/shared/diagnostics/common_support/dependency_checks.py`.

Move these exact complete function definitions from `lib/shared/diagnostics/common.py`:

- `check_circular_reference`
- `check_dependency_has_error`
- `check_transitive_scope_leak`
- `check_nested_view_chain`

Use this import block:

```python
from __future__ import annotations

from collections import deque

from shared.diagnostics import CatalogContext, DiagnosticResult, _THRESHOLDS, diagnostic
from shared.env_config import resolve_catalog_dir
from shared.diagnostics.common_support.graph import _get_dep_fqns, _load_catalog_json
```

- [ ] **Step 8: Convert `common.py` to registration facade**

Replace `lib/shared/diagnostics/common.py` with imports that load decorated functions for registration:

```python
"""Cross-dialect diagnostic check registration facade."""

from __future__ import annotations

from shared.diagnostics.common_support.dependency_checks import (  # noqa: F401
    check_circular_reference,
    check_dependency_has_error,
    check_nested_view_chain,
    check_transitive_scope_leak,
)
from shared.diagnostics.common_support.object_checks import (  # noqa: F401
    check_multi_table_read,
    check_multi_table_write,
    check_parse_error,
    check_stale_object,
    check_unsupported_syntax,
)
from shared.diagnostics.common_support.reference_checks import (  # noqa: F401
    check_missing_reference,
    check_out_of_scope_reference,
    check_remote_exec_unsupported,
)
```

- [ ] **Step 9: Run focused diagnostics tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/diagnostics -q
```

Expected: all diagnostics tests pass.

- [ ] **Step 10: Commit Task 3**

Run:

```bash
git add \
  lib/shared/diagnostics/common.py \
  lib/shared/diagnostics/common_support/__init__.py \
  lib/shared/diagnostics/common_support/object_checks.py \
  lib/shared/diagnostics/common_support/reference_checks.py \
  lib/shared/diagnostics/common_support/graph.py \
  lib/shared/diagnostics/common_support/dependency_checks.py \
  tests/unit/diagnostics/test_diagnostics.py
git commit -m "refactor: split common diagnostics checks"
```

---

### Task 4: Split `shared.loader_io`

**Files:**

- Create: `lib/shared/loader_io_support/__init__.py`
- Create: `lib/shared/loader_io_support/manifest.py`
- Create: `lib/shared/loader_io_support/directory.py`
- Create: `lib/shared/loader_io_support/indexing.py`
- Create: `lib/shared/loader_io_support/load.py`
- Modify: `lib/shared/loader_io.py`
- Test: `tests/unit/loader_io/test_loader_io.py`

- [ ] **Step 1: Write import-boundary tests**

Append to `tests/unit/loader_io/test_loader_io.py`:

```python
def test_loader_io_support_exports_manifest_reader(tmp_path: Path) -> None:
    from shared.loader_io_support.manifest import read_manifest

    assert read_manifest(tmp_path)["dialect"] == "tsql"
```

- [ ] **Step 2: Run the new test and verify it fails before support modules exist**

Run:

```bash
cd lib && uv run pytest ../tests/unit/loader_io/test_loader_io.py::test_loader_io_support_exports_manifest_reader -q
```

Expected: fail with `ModuleNotFoundError: No module named 'shared.loader_io_support'`.

- [ ] **Step 3: Create support package exports**

Create `lib/shared/loader_io_support/__init__.py`:

```python
"""Support modules for shared.loader_io."""
```

- [ ] **Step 4: Move manifest helpers**

Create `lib/shared/loader_io_support/manifest.py`.

Move these exact complete function definitions from `lib/shared/loader_io.py`:

- `read_manifest`
- `_require_manifest_file`
- `write_manifest_sandbox`
- `clear_manifest_sandbox`

Use this import block:

```python
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.runtime_config import (
    get_primary_dialect,
    get_runtime_role,
    set_runtime_role,
    validate_supported_technologies,
)
from shared.runtime_config_models import RuntimeRole

logger = logging.getLogger(__name__)
```

- [ ] **Step 5: Move directory loading helpers**

Create `lib/shared/loader_io_support/directory.py`.

Move these exact complete objects from `lib/shared/loader_io.py`:

- `_SEMICOLON_RE`
- `_DELIMITER_MAP`
- `_load_file`
- `load_directory`

Use this import block:

```python
from __future__ import annotations

import logging
import re
from pathlib import Path

from sqlglot import exp

from shared.env_config import resolve_ddl_dir
from shared.loader_data import DdlCatalog, DdlEntry, DdlParseError
from shared.loader_io_support.manifest import read_manifest
from shared.loader_parse import GO_RE, extract_name, extract_refs, extract_type_bucket, parse_block, split_blocks
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)
```

- [ ] **Step 6: Move indexing helpers**

Create `lib/shared/loader_io_support/indexing.py`.

Move these exact complete objects from `lib/shared/loader_io.py`:

- `_CATALOG_SCHEMA_VERSION`
- `_write_per_object_files`
- `index_directory`
- `load_catalog`

Use this import block:

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogLoadError, CatalogNotFoundError, DdlCatalog, DdlEntry
from shared.loader_io_support.directory import load_directory
from shared.name_resolver import normalize
```

- [ ] **Step 7: Move DDL loading orchestration**

Create `lib/shared/loader_io_support/load.py`.

Move this exact complete function definition from `lib/shared/loader_io.py`:

- `load_ddl`

Use this import block:

```python
from __future__ import annotations

import logging
from pathlib import Path

from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogNotFoundError, DdlCatalog
from shared.loader_io_support.directory import load_directory
from shared.loader_io_support.indexing import load_catalog
from shared.loader_io_support.manifest import read_manifest

logger = logging.getLogger(__name__)
```

- [ ] **Step 8: Convert `loader_io.py` to compatibility barrel**

Replace moved function bodies in `lib/shared/loader_io.py` with:

```python
"""Directory loading, catalog indexing, and on-disk I/O for DDL files."""

from __future__ import annotations

from shared.loader_io_support.directory import _load_file, load_directory
from shared.loader_io_support.indexing import _write_per_object_files, index_directory, load_catalog
from shared.loader_io_support.load import load_ddl
from shared.loader_io_support.manifest import (
    _require_manifest_file,
    clear_manifest_sandbox,
    read_manifest,
    write_manifest_sandbox,
)

__all__ = [
    "_load_file",
    "_require_manifest_file",
    "_write_per_object_files",
    "clear_manifest_sandbox",
    "index_directory",
    "load_catalog",
    "load_ddl",
    "load_directory",
    "read_manifest",
    "write_manifest_sandbox",
]
```

- [ ] **Step 9: Run focused loader tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/loader_io ../tests/unit/loader_parse -q
```

Expected: all loader tests pass.

- [ ] **Step 10: Commit Task 4**

Run:

```bash
git add \
  lib/shared/loader_io.py \
  lib/shared/loader_io_support/__init__.py \
  lib/shared/loader_io_support/manifest.py \
  lib/shared/loader_io_support/directory.py \
  lib/shared/loader_io_support/indexing.py \
  lib/shared/loader_io_support/load.py \
  tests/unit/loader_io/test_loader_io.py
git commit -m "refactor: split loader io support modules"
```

---

### Task 5: Integration Metadata and Verification

**Files:**

- Modify: `repo-map.json`

- [ ] **Step 1: Update `repo-map.json`**

In the `modules.shared_python.description` string, add these durable structural entries:

```text
profile.py (profiling CLI/barrel for table/view context and write commands), profile_support/ (seed profile helpers, table context assembly, view context assembly, and profile writeback)
catalog.py (catalog compatibility barrel), catalog_support/ (paths, typed loaders, merge helpers, reference-shape helpers, and catalog writers)
diagnostics/common.py (cross-dialect diagnostic registration facade), diagnostics/common_support/ (object checks, reference checks, graph helpers, dependency checks)
loader_io.py (DDL I/O compatibility barrel), loader_io_support/ (manifest helpers, DDL directory loading, catalog indexing/loading, load orchestration)
```

Keep the description concise. Do not add line counts.

- [ ] **Step 2: Validate JSON and line-count impact**

Run:

```bash
python -m json.tool repo-map.json >/dev/null
rg --files lib/shared mcp/ddl/ddl_mcp_support -g '*.py' -g '!**/.venv/**' -g '!**/__pycache__/**' \
  | xargs wc -l \
  | awk '$1 > 300 && $2 != "total" {print $1, $2}' \
  | sort -nr
```

Expected:

- `repo-map.json` is valid JSON.
- `lib/shared/profile.py`, `lib/shared/catalog.py`, `lib/shared/diagnostics/common.py`, and `lib/shared/loader_io.py` are no longer in the >300-line source list.
- New support files are each below 300 lines. If any new support file is above 300 lines, split that file before committing.

- [ ] **Step 3: Run focused test suites**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/profile \
  ../tests/unit/catalog \
  ../tests/unit/diagnostics \
  ../tests/unit/loader_io \
  ../tests/unit/loader_parse
```

Expected: all focused tests pass.

- [ ] **Step 4: Run repo Python checks**

Run:

```bash
uvx ruff check lib/shared packages/ad-migration-cli/src packages/ad-migration-internal/src mcp/ddl scripts tests/helpers.py tests/conftest.py tests mcp/ddl/tests --select F401,F841 --statistics
cd lib && GIT_CONFIG_GLOBAL=/dev/null uv run pytest
```

Expected:

- Ruff reports no unused imports or variables.
- Shared library tests pass.

- [ ] **Step 5: Commit integration metadata**

Run:

```bash
git add repo-map.json
git commit -m "docs: update module map for restructure pass two"
```

If `repo-map.json` was already committed by one of the workstream tasks, skip this commit and include the JSON validation output in the final handoff.

---

## Self-Review

Spec coverage:

- Worktree setup is complete at `/Users/hbanerjee/src/worktrees/modular-source-restructure-pass-2`.
- The plan targets the next >300-line source pass using current source inventory from merged `main`.
- The plan includes test restructuring only where it proves new module boundaries.
- The plan is written for parallel subagent workstreams and reserves metadata/verification for integration.

Placeholder scan:

- No forbidden placeholder markers or unspecified "add tests" steps remain.
- Every code-moving task names the exact functions to move and the exact destination file.
- Every task has concrete commands and expected outcomes.

Type consistency:

- Public compatibility imports preserve current callers of `shared.profile`, `shared.catalog`, `shared.diagnostics.common`, and `shared.loader_io`.
- New support packages use names that match existing module patterns from pass 1: `<module>_support/`.
- Test commands use the repo's `cd lib && uv run pytest <path>` convention from `repo-map.json`.
