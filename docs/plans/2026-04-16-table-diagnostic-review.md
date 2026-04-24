# Table Diagnostic Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a table-centric diagnostic review workflow so agents can investigate one table's `/status` warnings, fix catalog state when appropriate, or persist durable reviewed-warning artifacts that hide accepted warnings from default status output.

**Architecture:** Keep `/status` and `batch-plan` deterministic. Add a small reviewed-diagnostics support module that computes stable warning identities, reads/writes `catalog/diagnostic-reviews.json`, and filters matching reviewed warnings while reporting how many were hidden. Add a user-invocable skill that performs the agent reasoning loop for a single table FQN and uses the support module/artifact contract.

**Tech Stack:** Python 3.11+, Pydantic v2 output models, Typer CLI internals, JSON catalog files, repo-local Claude Code skill docs.

---

## File Structure

- Create `lib/shared/diagnostic_reviews.py`
  - Owns diagnostic identity hashing, review artifact path, review artifact loading/writing, and active/reviewed diagnostic partitioning.
- Modify `lib/shared/output_models/dry_run.py`
  - Adds reviewed-warning count metadata to `CatalogDiagnostics`.
- Modify `lib/shared/batch_plan.py`
  - Filters reviewed warnings from `catalog_diagnostics.warnings` by default and records `reviewed_warnings_hidden`.
- Create `tests/unit/diagnostic_reviews/test_diagnostic_reviews.py`
  - Unit tests for stable identity hashing, stale review behavior, and artifact writing.
- Modify `tests/unit/batch_plan/test_diagnostics_and_exclusions.py`
  - Integration-style unit tests proving batch-plan hides reviewed warnings, preserves errors, and reports hidden counts.
- Create `skills/reviewing-diagnostics/SKILL.md`
  - User-invocable table-centric workflow for `/review-diagnostics <table-fqn>`.
- Modify `repo-map.json`
  - Add the new skill and helper module to the appropriate existing descriptions.
- Modify `commands/status.md`
  - Update diagnostic triage guidance to point at `/review-diagnostics <table-fqn>` and mention reviewed warnings hidden by default.

---

### Task 1: Reviewed Diagnostic Support Module

**Files:**

- Create: `lib/shared/diagnostic_reviews.py`
- Test: `tests/unit/diagnostic_reviews/test_diagnostic_reviews.py`

- [ ] **Step 1: Write failing tests for identity and stale review behavior**

Create `tests/unit/diagnostic_reviews/test_diagnostic_reviews.py`:

```python
"""Tests for reviewed catalog diagnostic support."""

from __future__ import annotations

import json
from pathlib import Path

from shared.diagnostic_reviews import (
    DiagnosticIdentity,
    ReviewedDiagnostic,
    diagnostic_identity,
    load_reviewed_diagnostics,
    partition_reviewed_warnings,
    write_reviewed_diagnostic,
)


def test_diagnostic_identity_includes_fqn_code_and_message_hash() -> None:
    entry = {
        "code": "PARSE_ERROR",
        "message": "dynamic SQL reduced parse confidence",
        "severity": "warning",
    }

    identity = diagnostic_identity("gold.rpt_sales_by_category", entry)

    assert identity.fqn == "gold.rpt_sales_by_category"
    assert identity.code == "PARSE_ERROR"
    assert identity.message_hash.startswith("sha256:")
    assert identity.message_hash == diagnostic_identity("gold.rpt_sales_by_category", dict(entry)).message_hash
    changed = diagnostic_identity(
        "gold.rpt_sales_by_category",
        {**entry, "message": "different message"},
    )
    assert changed.message_hash != identity.message_hash


def test_write_and_load_reviewed_diagnostic_round_trips(tmp_path: Path) -> None:
    identity = DiagnosticIdentity(
        fqn="dim.dim_address",
        object_type="table",
        code="MULTI_TABLE_WRITE",
        message_hash="sha256:abc123",
    )

    written = write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **identity.model_dump(),
            status="accepted",
            reason="Reviewed table-specific slice; multi-table proc is intentional.",
            evidence=[
                "catalog/tables/dim.dim_address.json",
                "catalog/procedures/dbo.usp_load_dim_address_and_credit_card.json",
            ],
        ),
    )

    assert written == tmp_path / "catalog" / "diagnostic-reviews.json"
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "1.0"
    assert payload["reviews"][0]["fqn"] == "dim.dim_address"
    loaded = load_reviewed_diagnostics(tmp_path)
    assert loaded[0].reason == "Reviewed table-specific slice; multi-table proc is intentional."


def test_partition_reviewed_warnings_hides_only_matching_accepted_reviews(tmp_path: Path) -> None:
    active = [
        {
            "code": "MULTI_TABLE_WRITE",
            "message": "proc writes another table",
            "severity": "warning",
        },
        {
            "code": "PARSE_ERROR",
            "message": "parser fallback",
            "severity": "warning",
        },
    ]
    first_identity = diagnostic_identity("dim.dim_address", active[0], object_type="table")
    write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **first_identity.model_dump(),
            status="accepted",
            reason="Intentional multi-table writer.",
            evidence=[],
        ),
    )

    visible, hidden = partition_reviewed_warnings(
        tmp_path,
        fqn="dim.dim_address",
        object_type="table",
        warnings=active,
    )

    assert [warning["code"] for warning in visible] == ["PARSE_ERROR"]
    assert hidden == 1


def test_partition_reviewed_warnings_does_not_hide_changed_message(tmp_path: Path) -> None:
    old_entry = {
        "code": "MULTI_TABLE_WRITE",
        "message": "old warning text",
        "severity": "warning",
    }
    identity = diagnostic_identity("dim.dim_address", old_entry, object_type="table")
    write_reviewed_diagnostic(
        tmp_path,
        ReviewedDiagnostic(
            **identity.model_dump(),
            status="accepted",
            reason="Old review.",
            evidence=[],
        ),
    )

    visible, hidden = partition_reviewed_warnings(
        tmp_path,
        fqn="dim.dim_address",
        object_type="table",
        warnings=[{**old_entry, "message": "new warning text"}],
    )

    assert len(visible) == 1
    assert hidden == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd lib
uv run pytest ../tests/unit/diagnostic_reviews/test_diagnostic_reviews.py
```

Expected: FAIL because `shared.diagnostic_reviews` does not exist.

- [ ] **Step 3: Implement the support module**

Create `lib/shared/diagnostic_reviews.py`:

```python
"""Reviewed catalog diagnostic support.

Reviewed warnings are stored separately from source catalog warning entries so
catalog fixes and suppression rationale remain auditable.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.catalog import write_json

REVIEW_ARTIFACT = "diagnostic-reviews.json"
REVIEW_SCHEMA_VERSION = "1.0"


class DiagnosticIdentity(BaseModel):
    """Stable identity for one active catalog diagnostic."""

    fqn: str
    object_type: Literal["table", "view", "mv"] = "table"
    code: str
    message_hash: str


class ReviewedDiagnostic(DiagnosticIdentity):
    """One reviewed warning entry persisted under catalog/."""

    status: Literal["accepted"] = "accepted"
    reason: str
    evidence: list[str] = Field(default_factory=list)
    reviewed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    reviewed_by: str = "agent"


def review_artifact_path(project_root: Path) -> Path:
    """Return the project-local reviewed diagnostic artifact path."""
    return project_root / "catalog" / REVIEW_ARTIFACT


def _message_hash(message: str | None) -> str:
    raw = (message or "").strip().encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def diagnostic_identity(
    fqn: str,
    diagnostic: dict[str, Any],
    *,
    object_type: Literal["table", "view", "mv"] = "table",
) -> DiagnosticIdentity:
    """Build a stable diagnostic identity from a batch-plan warning entry."""
    code = diagnostic.get("code")
    if not isinstance(code, str) or not code:
        code = "UNKNOWN"
    message = diagnostic.get("message")
    return DiagnosticIdentity(
        fqn=fqn,
        object_type=object_type,
        code=code,
        message_hash=_message_hash(message if isinstance(message, str) else None),
    )


def load_reviewed_diagnostics(project_root: Path) -> list[ReviewedDiagnostic]:
    """Load accepted reviewed warnings from catalog/diagnostic-reviews.json."""
    path = review_artifact_path(project_root)
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return [ReviewedDiagnostic.model_validate(item) for item in data.get("reviews", [])]


def write_reviewed_diagnostic(project_root: Path, review: ReviewedDiagnostic) -> Path:
    """Upsert a reviewed warning entry and return the artifact path."""
    path = review_artifact_path(project_root)
    reviews = load_reviewed_diagnostics(project_root)
    replacement_key = (review.fqn, review.object_type, review.code, review.message_hash)
    kept = [
        existing
        for existing in reviews
        if (existing.fqn, existing.object_type, existing.code, existing.message_hash) != replacement_key
    ]
    payload = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "reviews": [
            item.model_dump(mode="json", exclude_none=True)
            for item in [*kept, review]
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)
    return path


def partition_reviewed_warnings(
    project_root: Path,
    *,
    fqn: str,
    object_type: Literal["table", "view", "mv"],
    warnings: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Return visible warnings and the count hidden by matching accepted reviews."""
    accepted = {
        (review.fqn, review.object_type, review.code, review.message_hash)
        for review in load_reviewed_diagnostics(project_root)
        if review.status == "accepted"
    }
    visible: list[dict[str, Any]] = []
    hidden = 0
    for warning in warnings:
        identity = diagnostic_identity(fqn, warning, object_type=object_type)
        key = (identity.fqn, identity.object_type, identity.code, identity.message_hash)
        if key in accepted:
            hidden += 1
        else:
            visible.append(warning)
    return visible, hidden
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd lib
uv run pytest ../tests/unit/diagnostic_reviews/test_diagnostic_reviews.py
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add lib/shared/diagnostic_reviews.py tests/unit/diagnostic_reviews/test_diagnostic_reviews.py
git commit -m "VU-1098: add diagnostic review artifact support"
```

---

### Task 2: Batch-Plan Reviewed Warning Filtering

**Files:**

- Modify: `lib/shared/output_models/dry_run.py`
- Modify: `lib/shared/batch_plan.py`
- Test: `tests/unit/batch_plan/test_diagnostics_and_exclusions.py`

- [ ] **Step 1: Write failing batch-plan tests**

Append to `tests/unit/batch_plan/test_diagnostics_and_exclusions.py`:

```python

class TestReviewedWarnings:
    def test_reviewed_warning_is_hidden_from_catalog_diagnostics(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        warning = {
            "code": "MULTI_TABLE_WRITE",
            "message": "proc also writes dim.other",
            "severity": "warning",
        }
        (tmp_path / "catalog" / "tables" / "dim.dim_address.json").write_text(
            json.dumps({
                "schema": "dim",
                "name": "dim_address",
                "warnings": [warning],
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import ReviewedDiagnostic, diagnostic_identity, write_reviewed_diagnostic

        identity = diagnostic_identity("dim.dim_address", warning, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Reviewed table slice and accepted multi-table writer.",
                evidence=["catalog/tables/dim.dim_address.json"],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_warnings == 0
        assert result.catalog_diagnostics.reviewed_warnings_hidden == 1

    def test_reviewed_warning_reappears_when_message_changes(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        old_warning = {"code": "PARSE_ERROR", "message": "old parse warning", "severity": "warning"}
        new_warning = {"code": "PARSE_ERROR", "message": "new parse warning", "severity": "warning"}
        (tmp_path / "catalog" / "tables" / "fact.fct_sales.json").write_text(
            json.dumps({
                "schema": "fact",
                "name": "fct_sales",
                "warnings": [new_warning],
                "scoping": {"status": "no_writer_found"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import ReviewedDiagnostic, diagnostic_identity, write_reviewed_diagnostic

        identity = diagnostic_identity("fact.fct_sales", old_warning, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Old review.",
                evidence=[],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_warnings == 1
        assert result.catalog_diagnostics.warnings[0].code == "PARSE_ERROR"
        assert result.catalog_diagnostics.reviewed_warnings_hidden == 0

    def test_errors_are_not_hidden_by_reviewed_warning_artifact(self, tmp_path: Path) -> None:
        (tmp_path / "catalog" / "tables").mkdir(parents=True)
        (tmp_path / "manifest.json").write_text(
            json.dumps({"schema_version": "1.0", "technology": "sql_server"}),
            encoding="utf-8",
        )
        error = {"code": "PARSE_ERROR", "message": "fatal parse issue", "severity": "error"}
        (tmp_path / "catalog" / "tables" / "fact.fct_sales.json").write_text(
            json.dumps({
                "schema": "fact",
                "name": "fct_sales",
                "errors": [error],
                "scoping": {"status": "error"},
            }),
            encoding="utf-8",
        )
        from shared.diagnostic_reviews import ReviewedDiagnostic, diagnostic_identity, write_reviewed_diagnostic

        identity = diagnostic_identity("fact.fct_sales", error, object_type="table")
        write_reviewed_diagnostic(
            tmp_path,
            ReviewedDiagnostic(
                **identity.model_dump(),
                status="accepted",
                reason="Attempted review should not suppress errors.",
                evidence=[],
            ),
        )

        result = build_batch_plan(tmp_path)

        assert result.catalog_diagnostics.total_errors == 1
        assert result.catalog_diagnostics.errors[0].code == "PARSE_ERROR"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd lib
uv run pytest ../tests/unit/batch_plan/test_diagnostics_and_exclusions.py -k ReviewedWarnings
```

Expected: FAIL because `reviewed_warnings_hidden` does not exist and batch-plan does not filter warnings.

- [ ] **Step 3: Extend the output model**

Modify `CatalogDiagnostics` in `lib/shared/output_models/dry_run.py`:

```python
class CatalogDiagnostics(BaseModel):
    model_config = OUTPUT_CONFIG

    total_errors: int
    total_warnings: int
    reviewed_warnings_hidden: int = 0
    warnings: list[CatalogDiagnosticEntry]
    errors: list[CatalogDiagnosticEntry]
```

- [ ] **Step 4: Filter reviewed warnings in batch-plan aggregation**

Modify `lib/shared/batch_plan.py`.

Add import:

```python
from shared.diagnostic_reviews import partition_reviewed_warnings
```

Replace the aggregation block with:

```python
    # Aggregate catalog diagnostics
    all_errors: list[CatalogDiagnosticEntry] = []
    all_warnings: list[CatalogDiagnosticEntry] = []
    reviewed_warnings_hidden = 0
    for fqn in sorted(obj_diagnostics):
        object_type = obj_type_map[fqn]
        visible_warnings, hidden_count = partition_reviewed_warnings(
            project_root,
            fqn=fqn,
            object_type=object_type,
            warnings=[
                diagnostic
                for diagnostic in obj_diagnostics[fqn]
                if diagnostic.get("severity") != "error"
            ],
        )
        reviewed_warnings_hidden += hidden_count
        visible_warning_keys = {
            (
                warning.get("code"),
                warning.get("message"),
                warning.get("severity", "warning"),
            )
            for warning in visible_warnings
        }
        for d in obj_diagnostics[fqn]:
            entry = CatalogDiagnosticEntry(fqn=fqn, object_type=object_type, **d)
            if d.get("severity") == "error":
                all_errors.append(entry)
            elif (
                d.get("code"),
                d.get("message"),
                d.get("severity", "warning"),
            ) in visible_warning_keys:
                all_warnings.append(entry)
```

Modify `_build_plan_output(...)` signature and call to accept `reviewed_warnings_hidden: int = 0`, then pass it into `CatalogDiagnostics`:

```python
catalog_diagnostics=CatalogDiagnostics(
    total_errors=len(all_errors or []),
    total_warnings=len(all_warnings or []),
    reviewed_warnings_hidden=reviewed_warnings_hidden,
    errors=all_errors or [],
    warnings=all_warnings or [],
)
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd lib
uv run pytest ../tests/unit/diagnostic_reviews/test_diagnostic_reviews.py ../tests/unit/batch_plan/test_diagnostics_and_exclusions.py -k "ReviewedWarnings or diagnostic"
```

Expected: PASS.

- [ ] **Step 6: Run full batch-plan tests**

Run:

```bash
cd lib
uv run pytest ../tests/unit/batch_plan
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

Run:

```bash
git add lib/shared/output_models/dry_run.py lib/shared/batch_plan.py tests/unit/batch_plan/test_diagnostics_and_exclusions.py
git commit -m "VU-1098: filter reviewed catalog warnings"
```

---

### Task 3: Table-Centric Diagnostic Review Skill

**Files:**

- Create: `skills/reviewing-diagnostics/SKILL.md`
- Modify: `repo-map.json`
- Modify: `commands/status.md`

- [ ] **Step 1: Create the skill document**

Create `skills/reviewing-diagnostics/SKILL.md`:

````markdown
---
name: reviewing-diagnostics
description: >
  Use when the user asks to review, clear, suppress, or investigate /status catalog diagnostics for a single table or migration object.
user-invocable: true
argument-hint: "<schema.table>"
---

# Reviewing Diagnostics

Review all active catalog diagnostics for one table and either fix catalog state, ask for human input, write a reviewed-warning artifact, or leave the warning active.

## Arguments

`$ARGUMENTS` is one table FQN such as `gold.rpt_sales_by_category`.

Ask for a table FQN if it is missing. Do not run this skill for all tables at once.

## Required Inputs

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate-util batch-plan
```

Find diagnostics where `fqn` matches the requested table.

Then load context:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" discover show --name <fqn>
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" discover refs --name <fqn>
```

Also inspect the relevant catalog JSON files directly when needed:

- `catalog/tables/<fqn>.json`
- selected writer from table scoping
- candidate or referenced writer procedures from `catalog/procedures/`
- `catalog/diagnostic-reviews.json`

## Decision Rules

Prefer fixes over suppression.

Fix catalog state when diagnostics show wrong or stale catalog facts, for example:

- wrong selected writer
- profile assigned to the wrong writer
- stale profile derived from incorrect scoping
- warning text says the actual writer appears to be another procedure

Ask the user when multiple plausible catalog fixes exist and the catalog does not identify one clear correction.

Write a reviewed-warning artifact only when:

- the warning is real but acceptable,
- the table-specific evidence was inspected,
- the warning does not block a safe migration path,
- the rationale is specific enough for a future maintainer.

Leave the warning active when the skill cannot prove either a fix or an acceptable suppression.

Never suppress errors by default.

## Reviewed Warning Artifact

Use `catalog/diagnostic-reviews.json`.

Each review must match the active warning by:

- table FQN
- object type
- diagnostic code
- message hash

The support module `shared.diagnostic_reviews` defines the exact identity and artifact shape.

## Workflow

1. List all active diagnostics for the table from batch-plan output.
2. Load the table catalog and related writer context.
3. Group diagnostics by likely root cause.
4. For each root cause, choose one outcome:
   - catalog fix
   - human choice
   - reviewed-warning artifact
   - leave active
5. Apply catalog fixes using the existing write commands when available. If no write command exists for the section being changed, edit the catalog JSON carefully and preserve unrelated fields.
6. For accepted warnings, write the review artifact with concrete evidence paths.
7. Re-run batch-plan and report remaining visible diagnostics for the table.

## Output

Report:

- diagnostics reviewed
- catalog fixes applied
- reviewed warnings accepted
- warnings left active and why
- next recommended command

## Common Mistakes

- Do not review by diagnostic code alone; always review one table at a time.
- Do not suppress warnings that point to wrong scoping or wrong profile state.
- Do not hide a warning by deleting it from the source catalog entry unless a catalog fix naturally removes it.
- Do not write vague reasons such as "reviewed" or "acceptable"; include the concrete evidence.
````

- [ ] **Step 2: Update `commands/status.md` diagnostic triage guidance**

In `commands/status.md` Step 6, add after the grouped remediation examples:

````markdown
For warnings that require human/agent review rather than immediate rerun commands, point the user to the table-centric diagnostic review skill:

```text
/review-diagnostics <schema.table>
```

If `catalog_diagnostics.reviewed_warnings_hidden > 0`, add:

```text
N reviewed warnings hidden - inspect catalog/diagnostic-reviews.json for rationale.
```
````

- [ ] **Step 3: Update `repo-map.json`**

In the `migration_skills` description, add `reviewing-diagnostics/` to the skill list with this concise text:

```text
reviewing-diagnostics/ (table-centric diagnostic review - inspects one table's status warnings, fixes catalog state when possible, or writes reviewed-warning artifacts)
```

In the `shared_python` description, mention:

```text
diagnostic_reviews.py (stable reviewed-warning identity, catalog/diagnostic-reviews.json read/write helpers, reviewed warning filtering support)
```

- [ ] **Step 4: Run markdown lint on changed Markdown**

Run:

```bash
markdownlint skills/reviewing-diagnostics/SKILL.md commands/status.md docs/superpowers/plans/2026-04-16-table-diagnostic-review.md
```

Expected: PASS. If markdownlint is unavailable, record that and run a manual Markdown formatting review.

- [ ] **Step 5: Commit Task 3**

Run:

```bash
git add skills/reviewing-diagnostics/SKILL.md commands/status.md repo-map.json docs/superpowers/plans/2026-04-16-table-diagnostic-review.md
git commit -m "VU-1098: add table diagnostic review skill"
```

---

### Task 4: Verification and Linear Update

**Files:**

- No planned source changes.
- Linear issue: `VU-1098`.

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd lib
uv run pytest ../tests/unit/diagnostic_reviews/test_diagnostic_reviews.py ../tests/unit/batch_plan/test_diagnostics_and_exclusions.py
```

Expected: PASS.

- [ ] **Step 2: Run full shared unit suite**

Run:

```bash
cd lib
uv run pytest
```

Expected: PASS.

- [ ] **Step 3: Run status-related smoke command against a fixture or local project**

Use an existing catalog fixture if available, otherwise use the active AdventureWorks migration project:

```bash
uv run --project packages/ad-migration-internal migrate-util batch-plan --project-root /Users/hbanerjee/src/adventureworks-migration
```

Expected: JSON output includes `catalog_diagnostics`. If `catalog/diagnostic-reviews.json` exists and matches active warnings, output includes a non-zero `reviewed_warnings_hidden`.

- [ ] **Step 4: Review acceptance criteria**

Confirm:

- A user-invocable skill exists for one table FQN.
- The skill instructions load active table warnings and related context.
- The skill prefers catalog fixes over suppression.
- Suppressions live in `catalog/diagnostic-reviews.json`.
- Matching uses FQN, code, object type, and message hash.
- Batch-plan hides matching reviewed warnings and reports hidden count.
- Errors remain visible.
- Tests cover filtering, stale reviews, and artifact writes.
- Status docs point users to the table-centric skill.

- [ ] **Step 5: Post Linear implementation note**

Post to `VU-1098`:

```markdown
Implemented table-centric diagnostic review support.

Changes:
- Added reviewed diagnostic identity/artifact helpers.
- Batch-plan now hides matching accepted reviewed warnings and reports reviewed warning count.
- Added `/review-diagnostics <table-fqn>` skill instructions.
- Updated `/status` guidance and repo map.

Verification:
- `cd lib && uv run pytest ../tests/unit/diagnostic_reviews/test_diagnostic_reviews.py ../tests/unit/batch_plan/test_diagnostics_and_exclusions.py`
- `cd lib && uv run pytest`
- status/batch-plan smoke: <result>
```

- [ ] **Step 6: Ensure worktree is clean**

Run:

```bash
git status --short --branch --untracked-files=all
```

Expected: no modified or untracked files.
