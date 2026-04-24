# Catalog Model Package Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the `shared.catalog_models` contract monolith into focused support modules while preserving every existing public import, validation rule, default, alias, and serialized shape.

**Architecture:** Keep `lib/shared/catalog_models.py` as the compatibility facade because Python cannot simultaneously import a sibling `catalog_models.py` file and a `catalog_models/` package. Put the split implementations in `lib/shared/catalog_model_support/`, then re-export the full public surface from `shared.catalog_models`. Use import-boundary and serialization tests before replacing the facade.

**Tech Stack:** Python 3.11, Pydantic v2, pytest, uv, existing `shared` package patterns.

---

## Worktree

All work for this plan happens in the existing worktree on this branch:

```text
/Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
branch: catalog-model-package-split
```

Do not edit `/Users/hbanerjee/src/migration-utility` for this work.

## Public Surface Inventory

`shared.catalog_models` currently exposes these contract symbols. The final facade must keep all of them importable from `shared.catalog_models`:

```text
_CATALOG_CONFIG
_STRICT_CONFIG
RefEntry
ScopedRefList
ReferencesBucket
ReferencedByBucket
StatementEntry
DiagnosticsEntry
ProfileDiagnosticsEntry
TableProfileStatus
ViewProfileStatus
ProfileSource
TableResolvedKind
PrimaryKeyType
ForeignKeyType
PiiSuggestedAction
ViewClassification
ViewProfileSource
CandidateWriter
TableScopingSection
SqlElement
ViewScopingSection
ScopingResultItem
ScopingSummaryCounts
ScopingSummary
ProfileClassification
ProfilePrimaryKey
ProfileNaturalKey
ProfileWatermark
ProfileForeignKey
ProfilePiiAction
TableProfileSection
ViewProfileSection
SemanticCheck
SemanticChecks
SemanticReview
CompareSqlSummary
RefactorSection
TestGenSection
GenerateSection
TableCatalog
ProcedureCatalog
ViewCatalog
FunctionCatalog
```

## Parallel Execution Model

This split has dependencies, so use two waves instead of launching every implementation task at once.

Wave 1:

- Task 1 creates support package foundations, shared reference/diagnostic/statement models, and facade boundary tests.

Wave 2, after Task 1 is committed:

- Task 2 owns scoping models.
- Task 3 owns profile models.
- Task 4 owns enriched/refactor/generation models.

Wave 3:

- Task 5 owns top-level catalog models and the `shared.catalog_models` compatibility facade.
- Task 6 owns integration metadata, full verification, and final review.

Use fresh subagents per task. Every implementer subagent must receive only its task text plus this global context:

```text
Repo root: /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup
Branch: catalog-model-package-split
Read AGENTS.md and repo-map.json before editing.
You are not alone in the codebase. Do not revert or modify changes outside your assigned Files section.
Preserve public imports from shared.catalog_models.
Copy model definitions exactly unless the task explicitly says to change imports.
Do not change Pydantic defaults, Field aliases, validators, Literal values, or model_config.
Use apply_patch for manual edits.
Run the task-specific tests before reporting DONE.
Commit only your assigned files with git add <file>.
```

Review each implementation task with two subagents before moving on:

1. Spec review: confirms file ownership, required symbols, and public compatibility for that task.
2. Code quality review: checks import cycles, accidental behavior changes, and test coverage.

## File Structure

### New Support Package

- Create: `lib/shared/catalog_model_support/__init__.py`
- Create: `lib/shared/catalog_model_support/base.py`
- Create: `lib/shared/catalog_model_support/references.py`
- Create: `lib/shared/catalog_model_support/statements.py`
- Create: `lib/shared/catalog_model_support/diagnostics.py`
- Create: `lib/shared/catalog_model_support/scoping.py`
- Create: `lib/shared/catalog_model_support/profile.py`
- Create: `lib/shared/catalog_model_support/enrichment.py`
- Create: `lib/shared/catalog_model_support/catalogs.py`

Responsibilities:

- `base.py`: `_CATALOG_CONFIG`, `_STRICT_CONFIG`.
- `references.py`: `RefEntry`, `ScopedRefList`, `ReferencesBucket`, `ReferencedByBucket`.
- `statements.py`: `StatementEntry`.
- `diagnostics.py`: `DiagnosticsEntry`, `ProfileDiagnosticsEntry`.
- `scoping.py`: `CandidateWriter`, `TableScopingSection`, `SqlElement`, `ViewScopingSection`, `ScopingResultItem`, `ScopingSummaryCounts`, `ScopingSummary`.
- `profile.py`: profile `Literal` aliases and profile section models.
- `enrichment.py`: semantic review, compare-sql, refactor, test-generation, and generation section models.
- `catalogs.py`: `TableCatalog`, `ProcedureCatalog`, `ViewCatalog`, `FunctionCatalog`.
- `catalog_models.py`: compatibility facade that imports and re-exports the full public surface.

## Task 1: Foundation, References, Diagnostics, and Public-Surface Tests

**Files:**

- Create: `lib/shared/catalog_model_support/__init__.py`
- Create: `lib/shared/catalog_model_support/base.py`
- Create: `lib/shared/catalog_model_support/references.py`
- Create: `lib/shared/catalog_model_support/statements.py`
- Create: `lib/shared/catalog_model_support/diagnostics.py`
- Test: `tests/unit/catalog_models/test_catalog_models_facade.py`
- Test: `tests/unit/catalog_models/test_reference_models.py`

- [ ] **Step 1: Write the public-surface compatibility test**

Create `tests/unit/catalog_models/test_catalog_models_facade.py`:

```python
"""Compatibility tests for the shared.catalog_models facade."""

from __future__ import annotations


def test_catalog_models_facade_exports_current_public_surface() -> None:
    import shared.catalog_models as models

    expected = [
        "_CATALOG_CONFIG",
        "_STRICT_CONFIG",
        "RefEntry",
        "ScopedRefList",
        "ReferencesBucket",
        "ReferencedByBucket",
        "StatementEntry",
        "DiagnosticsEntry",
        "ProfileDiagnosticsEntry",
        "TableProfileStatus",
        "ViewProfileStatus",
        "ProfileSource",
        "TableResolvedKind",
        "PrimaryKeyType",
        "ForeignKeyType",
        "PiiSuggestedAction",
        "ViewClassification",
        "ViewProfileSource",
        "CandidateWriter",
        "TableScopingSection",
        "SqlElement",
        "ViewScopingSection",
        "ScopingResultItem",
        "ScopingSummaryCounts",
        "ScopingSummary",
        "ProfileClassification",
        "ProfilePrimaryKey",
        "ProfileNaturalKey",
        "ProfileWatermark",
        "ProfileForeignKey",
        "ProfilePiiAction",
        "TableProfileSection",
        "ViewProfileSection",
        "SemanticCheck",
        "SemanticChecks",
        "SemanticReview",
        "CompareSqlSummary",
        "RefactorSection",
        "TestGenSection",
        "GenerateSection",
        "TableCatalog",
        "ProcedureCatalog",
        "ViewCatalog",
        "FunctionCatalog",
    ]

    missing = [name for name in expected if not hasattr(models, name)]
    assert missing == []
```

- [ ] **Step 2: Write the reference support import test**

Create `tests/unit/catalog_models/test_reference_models.py`:

```python
"""Tests for reference, statement, and diagnostic catalog model support modules."""

from __future__ import annotations

import pytest
from pydantic import ValidationError


def test_reference_support_modules_export_foundation_models() -> None:
    from shared.catalog_model_support.base import _CATALOG_CONFIG, _STRICT_CONFIG
    from shared.catalog_model_support.diagnostics import DiagnosticsEntry, ProfileDiagnosticsEntry
    from shared.catalog_model_support.references import (
        RefEntry,
        ReferencedByBucket,
        ReferencesBucket,
        ScopedRefList,
    )
    from shared.catalog_model_support.statements import StatementEntry

    assert _CATALOG_CONFIG["extra"] == "forbid"
    assert _STRICT_CONFIG["extra"] == "forbid"
    assert RefEntry.model_validate({"schema": "dbo", "name": "DimCustomer"}).object_schema == "dbo"
    assert ScopedRefList().in_scope == []
    assert ReferencesBucket().tables.in_scope == []
    assert ReferencedByBucket().procedures.in_scope == []
    assert StatementEntry(action="insert", source="catalog", sql="SELECT 1").sql == "SELECT 1"
    assert DiagnosticsEntry(code="X", message="m", severity="warning").severity == "warning"
    assert ProfileDiagnosticsEntry(code="X", message="m", severity="medium").severity == "medium"


def test_reference_support_models_preserve_strict_validation() -> None:
    from shared.catalog_model_support.diagnostics import DiagnosticsEntry
    from shared.catalog_model_support.references import RefEntry, ScopedRefList

    with pytest.raises(ValidationError, match="extra_forbidden"):
        ScopedRefList(in_scope=[], out_of_scope=[], unexpected=[])

    with pytest.raises(ValidationError, match="severity"):
        DiagnosticsEntry(code="X", message="m", severity="info")

    ref = RefEntry.model_validate({"schema": "dbo", "name": "FactSales"})
    assert ref.model_dump(by_alias=True)["schema"] == "dbo"
```

- [ ] **Step 3: Run the new support tests and verify they fail**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_catalog_models_facade.py \
  ../tests/unit/catalog_models/test_reference_models.py -q
```

Expected: `test_catalog_models_facade.py` passes against the old module, and `test_reference_models.py` fails with `ModuleNotFoundError: No module named 'shared.catalog_model_support'`.

- [ ] **Step 4: Create `base.py`**

Create `lib/shared/catalog_model_support/base.py`:

```python
"""Shared Pydantic config for catalog contract models."""

from __future__ import annotations

from pydantic import ConfigDict

_CATALOG_CONFIG = ConfigDict(extra="forbid", populate_by_name=True)
_STRICT_CONFIG = ConfigDict(extra="forbid")

__all__ = ["_CATALOG_CONFIG", "_STRICT_CONFIG"]
```

- [ ] **Step 5: Create `references.py`**

Move these definitions unchanged from `lib/shared/catalog_models.py` into `lib/shared/catalog_model_support/references.py`:

```text
RefEntry
ScopedRefList
ReferencesBucket
ReferencedByBucket
```

Use these imports:

```python
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from shared.catalog_model_support.base import _CATALOG_CONFIG, _STRICT_CONFIG
```

Add:

```python
__all__ = ["RefEntry", "ReferencedByBucket", "ReferencesBucket", "ScopedRefList"]
```

- [ ] **Step 6: Create `statements.py`**

Move `StatementEntry` unchanged into `lib/shared/catalog_model_support/statements.py`.

Use these imports:

```python
from __future__ import annotations

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
```

Add:

```python
__all__ = ["StatementEntry"]
```

- [ ] **Step 7: Create `diagnostics.py`**

Move these definitions unchanged into `lib/shared/catalog_model_support/diagnostics.py`:

```text
DiagnosticsEntry
ProfileDiagnosticsEntry
```

Use these imports:

```python
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
```

Add:

```python
__all__ = ["DiagnosticsEntry", "ProfileDiagnosticsEntry"]
```

- [ ] **Step 8: Create package `__init__.py`**

Create `lib/shared/catalog_model_support/__init__.py`:

```python
"""Focused support modules for the shared.catalog_models compatibility facade."""
```

- [ ] **Step 9: Run Task 1 tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_catalog_models_facade.py \
  ../tests/unit/catalog_models/test_reference_models.py \
  ../tests/unit/catalog_models/test_scoping_models.py -q
```

Expected: PASS.

- [ ] **Step 10: Commit Task 1**

Run:

```bash
git add \
  lib/shared/catalog_model_support/__init__.py \
  lib/shared/catalog_model_support/base.py \
  lib/shared/catalog_model_support/references.py \
  lib/shared/catalog_model_support/statements.py \
  lib/shared/catalog_model_support/diagnostics.py \
  tests/unit/catalog_models/test_catalog_models_facade.py \
  tests/unit/catalog_models/test_reference_models.py
git commit -m "refactor: add catalog model support foundations"
```

## Task 2: Split Scoping Models

**Files:**

- Create: `lib/shared/catalog_model_support/scoping.py`
- Test: `tests/unit/catalog_models/test_scoping_support.py`

- [ ] **Step 1: Write the support-module import test**

Create `tests/unit/catalog_models/test_scoping_support.py`:

```python
"""Tests for scoping catalog model support module exports."""

from __future__ import annotations


def test_scoping_support_exports_scoping_models() -> None:
    from shared.catalog_model_support.scoping import (
        CandidateWriter,
        ScopingResultItem,
        ScopingSummary,
        ScopingSummaryCounts,
        SqlElement,
        TableScopingSection,
        ViewScopingSection,
    )

    section = TableScopingSection(
        status="resolved",
        candidates=[CandidateWriter(procedure_name="dbo.usp_load", rationale="Direct writer.")],
    )
    summary = ScopingSummary(
        schema_version="1.0",
        run_id="run-1",
        results=[ScopingResultItem(item_id="silver.t", status="resolved")],
        summary=ScopingSummaryCounts(
            total=1,
            resolved=1,
            ambiguous_multi_writer=0,
            no_writer_found=0,
            analyzed=0,
            error=0,
        ),
    )

    assert section.candidates[0].procedure_name == "dbo.usp_load"
    assert SqlElement(type="join", detail="JOIN x").type == "join"
    assert ViewScopingSection(status="analyzed").status == "analyzed"
    assert summary.results[0].status == "resolved"
```

- [ ] **Step 2: Run the new support test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/catalog_models/test_scoping_support.py -q
```

Expected: FAIL with `ModuleNotFoundError` for `shared.catalog_model_support.scoping`.

- [ ] **Step 3: Create `scoping.py`**

Move these definitions unchanged into `lib/shared/catalog_model_support/scoping.py`:

```text
CandidateWriter
TableScopingSection
SqlElement
ViewScopingSection
ScopingResultItem
ScopingSummaryCounts
ScopingSummary
```

Use these imports:

```python
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import DiagnosticsEntry
```

Add:

```python
__all__ = [
    "CandidateWriter",
    "ScopingResultItem",
    "ScopingSummary",
    "ScopingSummaryCounts",
    "SqlElement",
    "TableScopingSection",
    "ViewScopingSection",
]
```

- [ ] **Step 4: Run scoping tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_scoping_support.py \
  ../tests/unit/catalog_models/test_scoping_models.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add \
  lib/shared/catalog_model_support/scoping.py \
  tests/unit/catalog_models/test_scoping_support.py
git commit -m "refactor: split catalog scoping models"
```

## Task 3: Split Profile Models

**Files:**

- Create: `lib/shared/catalog_model_support/profile.py`
- Test: `tests/unit/catalog_models/test_profile_support.py`

- [ ] **Step 1: Write the support-module import test**

Create `tests/unit/catalog_models/test_profile_support.py`:

```python
"""Tests for profile catalog model support module exports."""

from __future__ import annotations


def test_profile_support_exports_profile_models_and_aliases() -> None:
    from shared.catalog_model_support.profile import (
        ProfileClassification,
        ProfileForeignKey,
        ProfileNaturalKey,
        ProfilePiiAction,
        ProfilePrimaryKey,
        ProfileWatermark,
        TableProfileSection,
        ViewProfileSection,
    )

    table_profile = TableProfileSection.model_validate({
        "status": "ok",
        "classification": {
            "resolved_kind": "fact_insert",
            "source": "manual",
        },
        "primary_key": {"column": "LegacyKey", "columns": []},
        "natural_key": ["BusinessKey"],
        "watermark": {"columns": ["UpdatedAt"]},
        "foreign_keys": [{"columns": ["DimKey"], "references_table": "silver.dim"}],
        "pii_actions": [{"column": "Email", "action": "mask"}],
    })
    view_profile = ViewProfileSection.model_validate({
        "status": "ok",
        "classification": "mart",
        "rationale": "Aggregated view.",
        "source": "llm",
    })

    assert ProfileClassification().__class__.__name__ == "ProfileClassification"
    assert ProfilePrimaryKey(column="id").columns == ["id"]
    assert ProfileNaturalKey(columns=["id"]).columns == ["id"]
    assert ProfileWatermark(columns=["UpdatedAt"]).column == "UpdatedAt"
    assert ProfileForeignKey(columns=["DimKey"], references_table="silver.dim").references_source_relation == "silver.dim"
    assert ProfilePiiAction(column="Email", action="mask").suggested_action == "mask"
    assert table_profile.natural_key.columns == ["BusinessKey"]
    assert view_profile.classification == "mart"
```

- [ ] **Step 2: Run the new support test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/catalog_models/test_profile_support.py -q
```

Expected: FAIL with `ModuleNotFoundError` for `shared.catalog_model_support.profile`.

- [ ] **Step 3: Create `profile.py`**

Move these aliases and classes unchanged into `lib/shared/catalog_model_support/profile.py`:

```text
TableProfileStatus
ViewProfileStatus
ProfileSource
TableResolvedKind
PrimaryKeyType
ForeignKeyType
PiiSuggestedAction
ViewClassification
ViewProfileSource
ProfileClassification
ProfilePrimaryKey
ProfileNaturalKey
ProfileWatermark
ProfileForeignKey
ProfilePiiAction
TableProfileSection
ViewProfileSection
```

Use these imports:

```python
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import ProfileDiagnosticsEntry
```

Add every moved symbol to `__all__`.

- [ ] **Step 4: Run profile tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_profile_support.py \
  ../tests/unit/catalog_models/test_profile_models.py \
  ../tests/unit/profile/test_table_write.py \
  ../tests/unit/profile/test_view_write.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add \
  lib/shared/catalog_model_support/profile.py \
  tests/unit/catalog_models/test_profile_support.py
git commit -m "refactor: split catalog profile models"
```

## Task 4: Split Enrichment, Refactor, Test Generation, and Generation Models

**Files:**

- Create: `lib/shared/catalog_model_support/enrichment.py`
- Test: `tests/unit/catalog_models/test_enrichment_support.py`

- [ ] **Step 1: Write the support-module import test**

Create `tests/unit/catalog_models/test_enrichment_support.py`:

```python
"""Tests for enriched catalog section support module exports."""

from __future__ import annotations


def test_enrichment_support_exports_refactor_and_generation_models() -> None:
    from shared.catalog_model_support.enrichment import (
        CompareSqlSummary,
        GenerateSection,
        RefactorSection,
        SemanticCheck,
        SemanticChecks,
        SemanticReview,
        TestGenSection,
    )

    checks = SemanticChecks(
        source_tables=SemanticCheck(passed=True, summary="ok"),
        output_columns=SemanticCheck(passed=True, summary="ok"),
        joins=SemanticCheck(passed=True, summary="ok"),
        filters=SemanticCheck(passed=True, summary="ok"),
        aggregation_grain=SemanticCheck(passed=True, summary="ok"),
    )
    review = SemanticReview(passed=True, checks=checks)
    compare = CompareSqlSummary(
        required=True,
        executed=True,
        passed=True,
        scenarios_total=1,
        scenarios_passed=1,
    )
    refactor = RefactorSection(status="ok", semantic_review=review, compare_sql=compare)

    assert refactor.semantic_review.passed is True
    assert refactor.compare_sql.scenarios_passed == 1
    assert TestGenSection(status="ok").warnings == []
    assert GenerateSection(status="ok").errors == []
```

- [ ] **Step 2: Run the new support test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/catalog_models/test_enrichment_support.py -q
```

Expected: FAIL with `ModuleNotFoundError` for `shared.catalog_model_support.enrichment`.

- [ ] **Step 3: Create `enrichment.py`**

Move these definitions unchanged into `lib/shared/catalog_model_support/enrichment.py`:

```text
SemanticCheck
SemanticChecks
SemanticReview
CompareSqlSummary
RefactorSection
TestGenSection
GenerateSection
```

Use these imports:

```python
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG
from shared.catalog_model_support.diagnostics import DiagnosticsEntry
```

Add every moved symbol to `__all__`.

- [ ] **Step 4: Run enrichment/refactor tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_enrichment_support.py \
  ../tests/unit/refactor/test_models.py \
  ../tests/unit/migrate/test_write.py \
  ../tests/unit/test_harness/test_registry.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

Run:

```bash
git add \
  lib/shared/catalog_model_support/enrichment.py \
  tests/unit/catalog_models/test_enrichment_support.py
git commit -m "refactor: split enriched catalog section models"
```

## Task 5: Top-Level Catalog Models and Compatibility Facade

**Files:**

- Create: `lib/shared/catalog_model_support/catalogs.py`
- Modify: `lib/shared/catalog_models.py`
- Test: `tests/unit/catalog_models/test_catalog_model_support.py`
- Modify: `tests/unit/catalog_models/test_catalog_models_facade.py`

- [ ] **Step 1: Write top-level catalog support tests**

Create `tests/unit/catalog_models/test_catalog_model_support.py`:

```python
"""Tests for top-level catalog model support module exports."""

from __future__ import annotations


def test_catalog_support_exports_top_level_catalog_models() -> None:
    from shared.catalog_model_support.catalogs import (
        FunctionCatalog,
        ProcedureCatalog,
        TableCatalog,
        ViewCatalog,
    )

    table = TableCatalog.model_validate({"schema": "silver", "name": "DimCustomer"})
    proc = ProcedureCatalog.model_validate({"schema": "dbo", "name": "usp_load"})
    view = ViewCatalog.model_validate({"schema": "silver", "name": "vw_sales"})
    func = FunctionCatalog.model_validate({"schema": "dbo", "name": "fn_clean"})

    assert table.object_schema == "silver"
    assert table.model_dump(by_alias=True)["schema"] == "silver"
    assert proc.references is None
    assert view.is_materialized_view is False
    assert func.subtype is None
```

- [ ] **Step 2: Extend facade tests to assert identity with support modules**

Append this test to `tests/unit/catalog_models/test_catalog_models_facade.py`:

```python
def test_catalog_models_facade_reexports_support_module_classes_by_identity() -> None:
    import shared.catalog_models as models
    from shared.catalog_model_support.catalogs import TableCatalog
    from shared.catalog_model_support.diagnostics import DiagnosticsEntry
    from shared.catalog_model_support.enrichment import RefactorSection
    from shared.catalog_model_support.profile import TableProfileSection
    from shared.catalog_model_support.references import RefEntry
    from shared.catalog_model_support.scoping import TableScopingSection

    assert models.RefEntry is RefEntry
    assert models.DiagnosticsEntry is DiagnosticsEntry
    assert models.TableScopingSection is TableScopingSection
    assert models.TableProfileSection is TableProfileSection
    assert models.RefactorSection is RefactorSection
    assert models.TableCatalog is TableCatalog
```

- [ ] **Step 3: Run new tests and verify the support import fails**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models/test_catalog_model_support.py \
  ../tests/unit/catalog_models/test_catalog_models_facade.py -q
```

Expected before `catalogs.py`: FAIL with `ModuleNotFoundError` for `shared.catalog_model_support.catalogs`.

- [ ] **Step 4: Create `catalogs.py`**

Move these definitions unchanged into `lib/shared/catalog_model_support/catalogs.py`:

```text
TableCatalog
ProcedureCatalog
ViewCatalog
FunctionCatalog
```

Use these imports:

```python
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from shared.catalog_model_support.base import _CATALOG_CONFIG
from shared.catalog_model_support.enrichment import GenerateSection, RefactorSection, TestGenSection
from shared.catalog_model_support.profile import TableProfileSection, ViewProfileSection
from shared.catalog_model_support.references import ReferencedByBucket, ReferencesBucket
from shared.catalog_model_support.scoping import TableScopingSection, ViewScopingSection
```

Add:

```python
__all__ = ["FunctionCatalog", "ProcedureCatalog", "TableCatalog", "ViewCatalog"]
```

- [ ] **Step 5: Replace `catalog_models.py` with compatibility facade**

Replace `lib/shared/catalog_models.py` with imports and `__all__`. The facade must include all symbols from the Public Surface Inventory.

Use this shape:

```python
"""Compatibility facade for catalog Pydantic contract models."""

from __future__ import annotations

from shared.catalog_model_support.base import _CATALOG_CONFIG, _STRICT_CONFIG
from shared.catalog_model_support.catalogs import (
    FunctionCatalog,
    ProcedureCatalog,
    TableCatalog,
    ViewCatalog,
)
from shared.catalog_model_support.diagnostics import DiagnosticsEntry, ProfileDiagnosticsEntry
from shared.catalog_model_support.enrichment import (
    CompareSqlSummary,
    GenerateSection,
    RefactorSection,
    SemanticCheck,
    SemanticChecks,
    SemanticReview,
    TestGenSection,
)
from shared.catalog_model_support.profile import (
    ForeignKeyType,
    PiiSuggestedAction,
    PrimaryKeyType,
    ProfileClassification,
    ProfileForeignKey,
    ProfileNaturalKey,
    ProfilePiiAction,
    ProfilePrimaryKey,
    ProfileSource,
    ProfileWatermark,
    TableProfileSection,
    TableProfileStatus,
    TableResolvedKind,
    ViewClassification,
    ViewProfileSection,
    ViewProfileSource,
    ViewProfileStatus,
)
from shared.catalog_model_support.references import (
    RefEntry,
    ReferencedByBucket,
    ReferencesBucket,
    ScopedRefList,
)
from shared.catalog_model_support.scoping import (
    CandidateWriter,
    ScopingResultItem,
    ScopingSummary,
    ScopingSummaryCounts,
    SqlElement,
    TableScopingSection,
    ViewScopingSection,
)
from shared.catalog_model_support.statements import StatementEntry

__all__ = [
    "_CATALOG_CONFIG",
    "_STRICT_CONFIG",
    "CandidateWriter",
    "CompareSqlSummary",
    "DiagnosticsEntry",
    "ForeignKeyType",
    "FunctionCatalog",
    "GenerateSection",
    "PiiSuggestedAction",
    "PrimaryKeyType",
    "ProcedureCatalog",
    "ProfileClassification",
    "ProfileDiagnosticsEntry",
    "ProfileForeignKey",
    "ProfileNaturalKey",
    "ProfilePiiAction",
    "ProfilePrimaryKey",
    "ProfileSource",
    "ProfileWatermark",
    "RefEntry",
    "ReferencedByBucket",
    "ReferencesBucket",
    "RefactorSection",
    "ScopedRefList",
    "ScopingResultItem",
    "ScopingSummary",
    "ScopingSummaryCounts",
    "SemanticCheck",
    "SemanticChecks",
    "SemanticReview",
    "SqlElement",
    "StatementEntry",
    "TableCatalog",
    "TableProfileSection",
    "TableProfileStatus",
    "TableResolvedKind",
    "TableScopingSection",
    "TestGenSection",
    "ViewCatalog",
    "ViewClassification",
    "ViewProfileSection",
    "ViewProfileSource",
    "ViewProfileStatus",
    "ViewScopingSection",
]
```

- [ ] **Step 6: Run catalog model and downstream import tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models \
  ../tests/unit/catalog/test_catalog.py::test_catalog_facade_preserves_compatibility_exports \
  ../tests/unit/refactor/test_models.py \
  ../tests/unit/profile/test_table_write.py \
  ../tests/unit/profile/test_view_write.py \
  ../tests/unit/migrate/test_context.py \
  ../tests/unit/migrate/test_write.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Task 5**

Run:

```bash
git add \
  lib/shared/catalog_model_support/catalogs.py \
  lib/shared/catalog_models.py \
  tests/unit/catalog_models/test_catalog_model_support.py \
  tests/unit/catalog_models/test_catalog_models_facade.py
git commit -m "refactor: split top-level catalog models"
```

## Task 6: Integration Metadata, Verification, and Final Review

**Files:**

- Modify: `repo-map.json`

- [ ] **Step 1: Update `repo-map.json`**

Update the `shared_python` module description so it no longer says `catalog_models.py` owns all catalog model contracts. Use wording like:

```text
catalog_models.py (compatibility facade for catalog Pydantic contracts), catalog_model_support/ (domain-split catalog model contracts for references, diagnostics, scoping, profile, enriched generation/refactor sections, and top-level table/procedure/view/function catalogs)
```

- [ ] **Step 2: Validate JSON metadata**

Run:

```bash
python -m json.tool repo-map.json >/tmp/repo-map-check.json
```

Expected: exits `0`.

- [ ] **Step 3: Run public import compatibility smoke**

Run:

```bash
cd lib && uv run python - <<'PY'
from shared.catalog_models import (
    CandidateWriter,
    CompareSqlSummary,
    DiagnosticsEntry,
    FunctionCatalog,
    GenerateSection,
    ProcedureCatalog,
    ProfileDiagnosticsEntry,
    RefEntry,
    ReferencesBucket,
    RefactorSection,
    ScopingSummary,
    StatementEntry,
    TableCatalog,
    TableProfileSection,
    ViewCatalog,
    ViewProfileSection,
)

print(
    "catalog model imports ok",
    TableCatalog.__name__,
    ProcedureCatalog.__name__,
    ViewCatalog.__name__,
    FunctionCatalog.__name__,
    TableProfileSection.__name__,
    RefactorSection.__name__,
)
PY
```

Expected: exits `0` and prints `catalog model imports ok`.

- [ ] **Step 4: Run focused verification**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/catalog_models \
  ../tests/unit/catalog/test_catalog.py \
  ../tests/unit/profile \
  ../tests/unit/refactor/test_models.py \
  ../tests/unit/migrate/test_context.py \
  ../tests/unit/migrate/test_write.py \
  ../tests/unit/output_models/test_model_generation_models.py \
  ../tests/unit/output_models/test_test_spec_models.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit metadata**

Run:

```bash
git add repo-map.json
git commit -m "docs: update catalog model module map"
```

- [ ] **Step 6: Final code review**

Dispatch a read-only reviewer with this prompt:

```text
Review /Users/hbanerjee/src/worktrees/setup-ddl-boundary-cleanup on branch catalog-model-package-split.
Scope: catalog_models.py package split.
Inspect diff from origin/main..HEAD.
Focus on:
- public import compatibility for shared.catalog_models
- Pydantic validation, defaults, aliases, validators, and Literal values
- circular imports between catalog_model_support modules
- downstream import compatibility in catalog, profile, refactor, migrate, output_models, and dry_run modules
- whether tests prove representative serialized shapes are unchanged
Report findings first with file:line references. Do not edit files.
```

- [ ] **Step 7: Address review findings**

If the reviewer reports findings, fix them in the smallest owned files, rerun the failed or focused tests, and commit with a `fix:` message. If the reviewer reports no findings, proceed to branch completion.
