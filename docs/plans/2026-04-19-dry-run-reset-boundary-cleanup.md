# Dry Run Reset Boundary Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split destructive dry-run reset behavior into focused modules while preserving `run_reset_migration` behavior and CLI/import compatibility.

**Architecture:** Keep `lib/shared/dry_run_support/reset.py` as the public dispatcher/facade. Move filesystem deletion helpers into `reset_files.py`, staged table/procedure cleanup into `reset_stage.py`, and global non-preserve cleanup into `reset_global.py`; leave `reset_preserve_catalog.py` unchanged because it already owns the preserve-catalog global reset path.

**Tech Stack:** Python 3.11, Pydantic v2 output models, pytest, uv, existing `shared.dry_run_support` patterns.

---

## Worktree

All work happens in the existing worktree:

```text
/Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
branch: dry-run-reset-boundary-cleanup
```

Do not edit `/Users/hbanerjee/src/migration-utility` for this work.

## Scope

In scope:

- Split `lib/shared/dry_run_support/reset.py`.
- Preserve `shared.dry_run_support.reset.run_reset_migration`.
- Preserve `shared.dry_run_support.__init__`, `shared.dry_run_core`, and CLI behavior.
- Add import-boundary tests for each split module.
- Keep all destructive deletion behavior covered by existing and new tests.

Out of scope:

- Do not modify `lib/shared/dry_run_support/reset_preserve_catalog.py` except if a final reviewer finds a direct compatibility issue.
- Do not change output model shapes.
- Do not change reset semantics, path ordering, logging event names, or exception messages.

## Parallel Execution Model

Use three waves.

Wave 1:

- Task 1 creates shared filesystem helpers in `reset_files.py`.

Wave 2, after Task 1 is committed:

- Task 2 owns staged reset behavior in `reset_stage.py`.
- Task 3 owns global non-preserve reset behavior in `reset_global.py`.

Wave 3:

- Task 4 replaces `reset.py` with the public dispatcher/facade and updates monkeypatch targets in tests if needed.
- Task 5 updates `repo-map.json`, runs verification, and performs final review.

Use fresh subagents per task. Every implementer subagent must receive only its task text plus this global context:

```text
Repo root: /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
Branch: dry-run-reset-boundary-cleanup
Read AGENTS.md and repo-map.json before editing.
You are not alone in the codebase. Do not revert or modify changes outside your assigned Files section.
Preserve public imports and behavior for shared.dry_run_support.reset.run_reset_migration.
Do not change reset output ordering, logging event names, exception messages, or destructive semantics.
Use apply_patch for manual edits.
Run the task-specific tests before reporting DONE.
Commit only your assigned files with git add <file>.
```

Review each implementation task with two subagents before moving on:

1. Spec review: confirms file ownership, required behavior, and no out-of-scope edits.
2. Code quality review: checks import cycles, destructive-path safety, and test quality.

## File Structure

- Create: `lib/shared/dry_run_support/reset_files.py`
  Filesystem deletion helpers shared by staged and global reset modules.
- Create: `lib/shared/dry_run_support/reset_stage.py`
  Staged reset resolution, table section cleanup, writer refactor cleanup, and per-target output construction.
- Create: `lib/shared/dry_run_support/reset_global.py`
  Global non-preserve reset manifest cleanup and configured path deletion.
- Modify: `lib/shared/dry_run_support/reset.py`
  Public dispatcher/facade for `run_reset_migration`.
- Modify: `repo-map.json`
  Durable module responsibility update.

## Task 1: Split Reset Filesystem Helpers

**Files:**

- Create: `lib/shared/dry_run_support/reset_files.py`
- Test: `tests/unit/dry_run/test_reset_files.py`

- [ ] **Step 1: Write failing filesystem helper tests**

Create `tests/unit/dry_run/test_reset_files.py`:

```python
"""Tests for dry-run reset filesystem helpers."""

from __future__ import annotations


def test_delete_if_present_deletes_existing_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_if_present

    path = tmp_path / "state.json"
    path.write_text("{}", encoding="utf-8")

    assert delete_if_present(path) is True
    assert not path.exists()


def test_delete_if_present_reports_missing_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_if_present

    assert delete_if_present(tmp_path / "missing.json") is False


def test_delete_tree_if_present_deletes_directory_tree(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_tree_if_present

    path = tmp_path / "dbt" / "target"
    path.mkdir(parents=True)
    (path / "compiled.json").write_text("{}", encoding="utf-8")

    assert delete_tree_if_present(tmp_path / "dbt") is True
    assert not (tmp_path / "dbt").exists()


def test_delete_tree_if_present_deletes_file(tmp_path) -> None:
    from shared.dry_run_support.reset_files import delete_tree_if_present

    path = tmp_path / ".staging"
    path.write_text("legacy", encoding="utf-8")

    assert delete_tree_if_present(path) is True
    assert not path.exists()
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_reset_files.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dry_run_support.reset_files'`.

- [ ] **Step 3: Create `reset_files.py`**

Move the helper behavior from `reset.py` into `lib/shared/dry_run_support/reset_files.py` with public names:

```python
"""Filesystem helpers for dry-run reset operations."""

from __future__ import annotations

import shutil
from pathlib import Path


def delete_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    path.unlink()
    return True


def delete_tree_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


__all__ = ["delete_if_present", "delete_tree_if_present"]
```

- [ ] **Step 4: Run Task 1 tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_reset_files.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add \
  lib/shared/dry_run_support/reset_files.py \
  tests/unit/dry_run/test_reset_files.py
git commit -m "refactor: add dry run reset file helpers"
```

## Task 2: Split Staged Reset Behavior

**Files:**

- Create: `lib/shared/dry_run_support/reset_stage.py`
- Test: `tests/unit/dry_run/test_reset_stage.py`

- [ ] **Step 1: Write staged reset support tests**

Create `tests/unit/dry_run/test_reset_stage.py`:

```python
"""Tests for staged dry-run reset behavior."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.dry_run.dry_run_test_helpers import _make_reset_project


def test_reset_table_sections_preserves_scoping_and_deletes_test_spec(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import reset_table_sections

    root = _make_reset_project(tmp_path)

    cleared_sections, deleted_files, mutated_files, writer = reset_table_sections(
        root,
        "silver.dimcustomer",
        "profile",
    )

    assert writer == "dbo.usp_load_dimcustomer"
    assert "table.profile" in cleared_sections
    assert "table.test_gen" in cleared_sections
    assert deleted_files == ["test-specs/silver.dimcustomer.json"]
    assert mutated_files == ["catalog/tables/silver.dimcustomer.json"]
    table = json.loads((root / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "scoping" in table
    assert "profile" not in table


def test_reset_writer_refactor_removes_procedure_refactor(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import reset_writer_refactor

    root = _make_reset_project(tmp_path)

    cleared_sections, mutated_files = reset_writer_refactor(root, "dbo.usp_load_dimcustomer")

    assert cleared_sections == ["procedure:dbo.usp_load_dimcustomer.refactor"]
    assert mutated_files == ["catalog/procedures/dbo.usp_load_dimcustomer.json"]
    proc = json.loads((root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc


def test_run_reset_migration_stage_blocks_before_mutating_any_valid_target(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import run_reset_migration_stage

    root = _make_reset_project(tmp_path)
    blocked_table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
    blocked_table = json.loads(blocked_table_path.read_text(encoding="utf-8"))
    blocked_table["generate"] = {"status": "ok"}
    blocked_table_path.write_text(json.dumps(blocked_table), encoding="utf-8")

    result = run_reset_migration_stage(root, "profile", ["silver.DimCustomer", "silver.DimProduct"])

    assert result.reset == []
    assert result.blocked == ["silver.dimcustomer"]
    assert result.targets[0].status == "blocked"
    assert (root / "test-specs" / "silver.dimproduct.json").exists()
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_reset_stage.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dry_run_support.reset_stage'`.

- [ ] **Step 3: Create `reset_stage.py`**

Move these definitions from `reset.py` into `lib/shared/dry_run_support/reset_stage.py` and rename private helpers to public module-local helpers:

```text
_reset_table_sections -> reset_table_sections
_reset_writer_refactor -> reset_writer_refactor
staged branch of run_reset_migration -> run_reset_migration_stage
```

Use these imports:

```python
from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.catalog import detect_catalog_bucket, write_json
from shared.dry_run_support.common import RESETTABLE_STAGES, _RESET_STAGE_SECTIONS, read_catalog_json
from shared.dry_run_support.reset_files import delete_if_present
from shared.name_resolver import normalize
from shared.output_models.dry_run import ResetMigrationOutput, ResetTargetResult
```

`run_reset_migration_stage` must preserve current staged-reset semantics:

```python
def run_reset_migration_stage(project_root: Path, stage: str, fqns: list[str]) -> ResetMigrationOutput:
    if stage not in RESETTABLE_STAGES:
        raise ValueError(f"Unsupported reset stage: {stage}")
    if not fqns:
        raise ValueError("reset-migration requires at least one FQN for staged resets")
    ...
```

Do not change target ordering, blocked-before-mutation behavior, `not_found` behavior, or output field values.

- [ ] **Step 4: Run staged reset tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/dry_run/test_reset_stage.py \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_profile_clears_downstream_and_preserves_scoping \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_refactor_only_clears_writer_refactor \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_is_idempotent_noop \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_multiple_tables \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_blocks_model_complete_before_mutation \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_not_found_returns_without_mutation \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_mixed_valid_and_missing_resets_valid_targets \
  ../tests/unit/dry_run/test_reset_migration.py::test_reset_migration_requires_at_least_one_fqn -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add \
  lib/shared/dry_run_support/reset_stage.py \
  tests/unit/dry_run/test_reset_stage.py
git commit -m "refactor: split staged dry run reset"
```

## Task 3: Split Global Non-Preserve Reset Behavior

**Files:**

- Create: `lib/shared/dry_run_support/reset_global.py`
- Test: `tests/unit/dry_run/test_reset_global.py`

- [ ] **Step 1: Write global reset support tests**

Create `tests/unit/dry_run/test_reset_global.py`:

```python
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
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_reset_global.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'shared.dry_run_support.reset_global'`.

- [ ] **Step 3: Create `reset_global.py`**

Move these definitions from `reset.py` into `lib/shared/dry_run_support/reset_global.py` and rename private helpers to public module-local helpers:

```text
_prepare_reset_migration_all_manifest -> prepare_reset_migration_all_manifest
_run_reset_migration_all -> run_reset_migration_all
```

Use these imports:

```python
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import write_json
from shared.dry_run_support.common import RESET_GLOBAL_MANIFEST_SECTIONS, RESET_GLOBAL_PATHS
from shared.dry_run_support.reset_files import delete_tree_if_present
from shared.output_models.dry_run import ResetMigrationOutput

logger = logging.getLogger(__name__)
```

Preserve current logging event names, deleted/missing path ordering, manifest write behavior, and invalid-manifest failure behavior.

- [ ] **Step 4: Run global reset tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/dry_run/test_reset_global.py \
  ../tests/unit/dry_run/test_reset_migration.py::test_reset_migration_global_output_contract_serializes_deleted_paths \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_all_reports_missing_paths_as_noop \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_all_invalid_manifest_preserves_directories \
  ../tests/unit/dry_run/test_reset_migration.py::test_run_reset_migration_all_rejects_extra_table_arguments -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add \
  lib/shared/dry_run_support/reset_global.py \
  tests/unit/dry_run/test_reset_global.py
git commit -m "refactor: split global dry run reset"
```

## Task 4: Replace Reset Module With Dispatcher Facade

**Files:**

- Modify: `lib/shared/dry_run_support/reset.py`
- Test: `tests/unit/dry_run/test_reset_boundaries.py`
- Modify: `tests/unit/dry_run/test_reset_migration.py` only if monkeypatch targets need to point at owning modules.

- [ ] **Step 1: Write reset facade boundary tests**

Create `tests/unit/dry_run/test_reset_boundaries.py`:

```python
"""Boundary tests for dry-run reset split modules and facade."""

from __future__ import annotations


def test_reset_facade_exports_public_dispatcher_and_legacy_helpers() -> None:
    from shared.dry_run_support import reset

    assert callable(reset.run_reset_migration)
    assert callable(reset.delete_if_present)
    assert callable(reset.delete_tree_if_present)
    assert callable(reset.reset_table_sections)
    assert callable(reset.reset_writer_refactor)
    assert callable(reset.prepare_reset_migration_all_manifest)
    assert callable(reset.run_reset_migration_all)


def test_reset_support_modules_export_owned_entrypoints() -> None:
    from shared.dry_run_support.reset_files import delete_if_present, delete_tree_if_present
    from shared.dry_run_support.reset_global import (
        prepare_reset_migration_all_manifest,
        run_reset_migration_all,
    )
    from shared.dry_run_support.reset_stage import (
        reset_table_sections,
        reset_writer_refactor,
        run_reset_migration_stage,
    )

    assert callable(delete_if_present)
    assert callable(delete_tree_if_present)
    assert callable(prepare_reset_migration_all_manifest)
    assert callable(run_reset_migration_all)
    assert callable(reset_table_sections)
    assert callable(reset_writer_refactor)
    assert callable(run_reset_migration_stage)
```

- [ ] **Step 2: Run boundary tests before facade replacement**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_reset_boundaries.py -q
```

Expected before Step 3: may fail because `reset.py` has not yet exposed renamed helper aliases. Continue to Step 3.

- [ ] **Step 3: Replace `reset.py` with dispatcher/facade**

Replace implementation in `lib/shared/dry_run_support/reset.py` with a focused dispatcher:

```python
from __future__ import annotations

from pathlib import Path

from shared.dry_run_support.common import RESETTABLE_STAGES
from shared.dry_run_support.reset_files import delete_if_present, delete_tree_if_present
from shared.dry_run_support.reset_global import (
    prepare_reset_migration_all_manifest,
    run_reset_migration_all,
)
from shared.dry_run_support.reset_preserve_catalog import run_reset_migration_all_preserve_catalog
from shared.dry_run_support.reset_stage import (
    reset_table_sections,
    reset_writer_refactor,
    run_reset_migration_stage,
)
from shared.output_models.dry_run import ResetMigrationOutput


def run_reset_migration(
    project_root: Path,
    stage: str,
    fqns: list[str],
    *,
    preserve_catalog: bool = False,
) -> ResetMigrationOutput:
    """Reset pre-model migration state for one or more selected tables."""
    if stage == "all":
        if fqns:
            raise ValueError("global reset stage 'all' does not accept table arguments")
        if preserve_catalog:
            return run_reset_migration_all_preserve_catalog(project_root)
        return run_reset_migration_all(project_root)
    if preserve_catalog:
        raise ValueError("--preserve-catalog is only supported with global reset stage 'all'")
    return run_reset_migration_stage(project_root, stage, fqns)


__all__ = [
    "RESETTABLE_STAGES",
    "delete_if_present",
    "delete_tree_if_present",
    "prepare_reset_migration_all_manifest",
    "reset_table_sections",
    "reset_writer_refactor",
    "run_reset_migration",
    "run_reset_migration_all",
    "run_reset_migration_all_preserve_catalog",
    "run_reset_migration_stage",
]
```

Keep legacy helper aliases importable from `shared.dry_run_support.reset`; do not keep old underscored names unless a grep shows callers import them.

- [ ] **Step 4: Run reset facade and dry-run contract tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/dry_run/test_reset_boundaries.py \
  ../tests/unit/dry_run/test_reset_files.py \
  ../tests/unit/dry_run/test_reset_stage.py \
  ../tests/unit/dry_run/test_reset_global.py \
  ../tests/unit/dry_run/test_reset_migration.py \
  ../tests/unit/dry_run/test_reset_preserve_catalog.py \
  ../tests/unit/dry_run/test_core_contract.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git add \
  lib/shared/dry_run_support/reset.py \
  tests/unit/dry_run/test_reset_boundaries.py \
  tests/unit/dry_run/test_reset_migration.py
git commit -m "refactor: make dry run reset dispatcher explicit"
```

Only include `tests/unit/dry_run/test_reset_migration.py` if Step 4 required monkeypatch target updates.

## Task 5: Integration Metadata, Verification, and Final Review

**Files:**

- Modify: `repo-map.json`

- [ ] **Step 1: Update `repo-map.json`**

Update the `shared_python` module description so the `dry_run_support/` entry names the split reset modules:

```text
dry_run_support/ (split migrate-util command services: readiness.py/reset.py orchestration, reset_stage.py staged reset behavior, reset_global.py global non-preserve reset behavior, reset_files.py reset filesystem helpers, readiness_context.py object context guards, readiness_stages.py per-stage policy, reset_preserve_catalog.py preserve-catalog global reset, status, exclusions, excluded warning reconciliation, and shared helpers)
```

- [ ] **Step 2: Validate metadata JSON**

Run:

```bash
python -m json.tool repo-map.json >/tmp/repo-map-check.json
```

Expected: exits `0`.

- [ ] **Step 3: Run public import smoke**

Run:

```bash
cd lib && uv run python - <<'PY'
from shared.dry_run_support.reset import run_reset_migration
from shared.dry_run_core import run_reset_migration as core_run_reset_migration
from shared.dry_run_support.reset_global import run_reset_migration_all
from shared.dry_run_support.reset_stage import run_reset_migration_stage

assert run_reset_migration is core_run_reset_migration
print("dry run reset imports ok", run_reset_migration.__name__, run_reset_migration_all.__name__, run_reset_migration_stage.__name__)
PY
```

Expected: exits `0` and prints `dry run reset imports ok`.

- [ ] **Step 4: Run focused verification**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/dry_run \
  ../tests/unit/cli/test_reset_cmd.py \
  ../tests/unit/cli/test_pipeline_cmds.py \
  ../tests/unit/dry_run/test_core_contract.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit metadata**

Run:

```bash
git add repo-map.json
git commit -m "docs: update dry run reset module map"
```

- [ ] **Step 6: Final code review**

Dispatch a read-only reviewer with this prompt:

```text
Review /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup on branch dry-run-reset-boundary-cleanup.
Scope: dry_run_support/reset.py boundary cleanup.
Inspect diff from origin/main..HEAD.
Focus on:
- destructive reset behavior preservation
- blocked-before-mutation behavior for staged resets
- global reset path ordering and manifest cleanup semantics
- preserve-catalog behavior staying unchanged
- public import compatibility for shared.dry_run_support.reset and shared.dry_run_core
- tests that monkeypatch the wrong module or overfit implementation details
- repo-map accuracy
Report findings first with file:line references. Do not edit files.
```

- [ ] **Step 7: Address review findings**

If the reviewer reports findings, fix them in the smallest relevant files, rerun focused tests, and commit with a `fix:` message. If there are no findings, proceed to PR preparation.
