# Seed Table Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add explicit seed-table catalog state, CLI mutation, seed profile persistence, and status/batch reporting distinct from source tables.

**Architecture:** Seed ownership is stored as `TableCatalog.is_seed`, mutually exclusive with `is_source`. The deterministic `add-seed-table` CLI flips catalog ownership and writes a seed profile; profiling/status/batch code then treats seed tables as intentional non-migration inputs without applying writer-driven profiling. Existing non-seed migration and source-table flows remain unchanged.

**Tech Stack:** Python 3.11, Typer CLI, Pydantic v2 catalog/output models, pytest unit tests, markdown command/skill specs, promptfoo command evals.

---

## Files And Responsibilities

- `lib/shared/catalog_models.py`: add strict table catalog contract for `is_seed`.
- `lib/shared/catalog_writer.py`: add seed write helper and enforce source/seed mutual exclusion.
- `lib/shared/output_models/writeback.py`: add seed write output contract.
- `lib/shared/cli/add_source_table_cmd.py`: keep command behavior but clear `is_seed` when marking a source.
- `lib/shared/cli/add_seed_table_cmd.py`: new CLI command that marks tables as seeds and writes seed profiles.
- `lib/shared/cli/main.py`: register `add-seed-table`.
- `lib/shared/profile.py`: accept seed classification and provide deterministic seed profile construction.
- `lib/shared/dry_run_core.py`: report seed tables as `SEED_TABLE` not-applicable objects.
- `lib/shared/pipeline_status.py`: classify seed tables as not applicable when queried directly.
- `lib/shared/batch_plan.py`: enumerate seed tables separately from source, writerless, and active pipeline objects.
- `lib/shared/output_models/dry_run.py`: add `seed_tables` summary/list output.
- `commands/profile.md`: update batch profiling command contract for seed-table skip/report behavior.
- `skills/profiling-table/SKILL.md`: update single-object profiling behavior for seed tables.
- `repo-map.json`: add the new CLI command entry.
- Tests under `tests/unit/`: cover catalog mutation, CLI routing, profile persistence, readiness, and batch output.

No integration tests are required because this changes catalog mutations and status calculations only. No manual tests required.

## Task 1: Catalog Contract And Seed Writer

**Files:**

- Modify: `lib/shared/catalog_models.py`
- Modify: `lib/shared/output_models/writeback.py`
- Modify: `lib/shared/output_models/catalog_writer.py`
- Modify: `lib/shared/profile.py`
- Modify: `lib/shared/catalog_writer.py`
- Test: `tests/unit/discover/test_discover.py`
- Test: `tests/unit/profile/test_profile.py`

- [ ] **Step 1: Add failing catalog-writer tests**

Append tests near the existing `run_write_source` tests in `tests/unit/discover/test_discover.py`:

```python
def test_write_source_clears_seed_flag() -> None:
    """run_write_source clears is_seed when marking a table as source."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {"is_seed": True, "is_source": False},
        )
        result = discover.run_write_source(root, "silver.lookup", True)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_source is True
        assert written["is_source"] is True
        assert written["is_seed"] is False


def test_write_seed_sets_seed_and_clears_source_with_profile() -> None:
    """run_write_seed marks a table as seed and persists seed profile semantics."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = discover.run_write_seed(root, "silver.lookup", True)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_seed is True
        assert written["is_seed"] is True
        assert written["is_source"] is False
        assert written["profile"]["status"] == "ok"
        assert written["profile"]["classification"]["resolved_kind"] == "seed"
        assert written["profile"]["classification"]["source"] == "catalog"


def test_write_seed_false_resets_flag_without_clearing_profile() -> None:
    """run_write_seed with value=False writes is_seed false and leaves other state alone."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {
                "is_seed": True,
                "is_source": False,
                "profile": {
                    "status": "ok",
                    "classification": {"resolved_kind": "seed", "source": "catalog"},
                },
            },
        )
        result = discover.run_write_seed(root, "silver.lookup", False)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_seed is False
        assert written["is_seed"] is False
        assert written["profile"]["classification"]["resolved_kind"] == "seed"
```

Add profile contract tests in `tests/unit/profile/test_profile.py` near table `run_write` tests:

```python
def test_write_seed_profile_allowed_for_seed_table() -> None:
    """Seed tables can persist a seed classification profile."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_seed"] = True
        cat["is_source"] = False
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = profile.run_write(
            root,
            "silver.FactSales",
            {"classification": {"resolved_kind": "seed", "source": "catalog", "rationale": "Static seed data."}},
        )
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result["table"] == "silver.factsales"
        assert written["profile"]["status"] == "ok"
        assert written["profile"]["classification"]["resolved_kind"] == "seed"


def test_write_seed_profile_rejected_for_non_seed_table() -> None:
    """Non-seed tables cannot be profiled with seed classification."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="seed classification requires is_seed"):
            profile.run_write(
                root,
                "silver.FactSales",
                {"classification": {"resolved_kind": "seed", "source": "catalog"}},
            )
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/discover/test_discover.py::test_write_source_clears_seed_flag ../tests/unit/discover/test_discover.py::test_write_seed_sets_seed_and_clears_source_with_profile ../tests/unit/discover/test_discover.py::test_write_seed_false_resets_flag_without_clearing_profile ../tests/unit/profile/test_profile.py::test_write_seed_profile_allowed_for_seed_table ../tests/unit/profile/test_profile.py::test_write_seed_profile_rejected_for_non_seed_table -q
```

Expected: failures because `is_seed`, `run_write_seed`, and seed profile validation do not exist.

- [ ] **Step 3: Add catalog/output models**

In `lib/shared/catalog_models.py`, add `is_seed: bool = False` immediately after the
existing `is_source: bool = False` field in `TableCatalog`.

In `lib/shared/output_models/writeback.py`, add this model after `WriteSourceOutput`:

```python
class WriteSeedOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    written: str
    is_seed: bool
    status: Literal["ok"]
```

In `lib/shared/output_models/catalog_writer.py`, re-export the new model:

```python
from shared.output_models.writeback import WriteSeedOutput, WriteSliceOutput, WriteSourceOutput

__all__ = ["WriteSeedOutput", "WriteSliceOutput", "WriteSourceOutput"]
```

- [ ] **Step 4: Add deterministic seed profile support**

In `lib/shared/profile.py`, include seed in the classification contract and add a helper:

```python
RESOLVED_KINDS = frozenset({
    "seed",
    "dim_non_scd",
    "dim_scd1",
    "dim_scd2",
    "dim_junk",
    "fact_transaction",
    "fact_periodic_snapshot",
    "fact_accumulating_snapshot",
    "fact_aggregate",
})


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

In `run_write`, after loading `existing_table` and before deriving status, reject accidental seed classification on non-seed tables:

```python
    classification = profile_json.get("classification")
    resolved_kind = classification.get("resolved_kind") if isinstance(classification, dict) else None
    if resolved_kind == "seed" and not existing_table.is_seed:
        raise ValueError(f"seed classification requires is_seed: true for {norm}")
```

Derive `status = "ok"` for seed profiles:

```python
    if resolved_kind == "seed":
        status = "ok"
    elif has_classification and has_primary_key:
        status = "ok"
    elif has_classification:
        status = "partial"
    else:
        status = "error"
```

- [ ] **Step 5: Implement mutual-exclusion writers**

In `lib/shared/catalog_writer.py`, import `WriteSeedOutput` and `build_seed_profile`. Update `run_write_source` to write both flags:

```python
    result = load_and_merge_catalog(project_root, table_norm, "is_source", value)
    if value:
        result = load_and_merge_catalog(project_root, table_norm, "is_seed", False)
```

Add `run_write_seed`:

```python
def run_write_seed(
    project_root: Path,
    table_fqn: str,
    value: bool,
) -> WriteSeedOutput:
    """Set or clear the is_seed flag on a table catalog file."""
    table_norm = normalize(table_fqn)
    cat_model = load_table_catalog(project_root, table_norm)
    if cat_model is None:
        raise CatalogFileMissingError("table", table_norm)

    if cat_model.scoping is None:
        raise ValueError(
            f"Table {table_norm!r} has not been analyzed yet. "
            "Run /analyzing-table first."
        )

    result = load_and_merge_catalog(project_root, table_norm, "is_seed", value)
    if value:
        result = load_and_merge_catalog(project_root, table_norm, "is_source", False)
        result = load_and_merge_catalog(project_root, table_norm, "profile", build_seed_profile())

    logger.info(
        "event=write_seed_complete component=catalog_writer operation=run_write_seed "
        "table=%s is_seed=%s status=success",
        table_norm,
        value,
    )

    return WriteSeedOutput(written=result["catalog_path"], is_seed=value, status="ok")
```

- [ ] **Step 6: Run task tests and commit**

Run:

```bash
cd lib && uv run pytest ../tests/unit/discover/test_discover.py::test_write_source_clears_seed_flag ../tests/unit/discover/test_discover.py::test_write_seed_sets_seed_and_clears_source_with_profile ../tests/unit/discover/test_discover.py::test_write_seed_false_resets_flag_without_clearing_profile ../tests/unit/profile/test_profile.py::test_write_seed_profile_allowed_for_seed_table ../tests/unit/profile/test_profile.py::test_write_seed_profile_rejected_for_non_seed_table -q
```

Expected: pass.

Commit:

```bash
git add lib/shared/catalog_models.py lib/shared/output_models/writeback.py lib/shared/output_models/catalog_writer.py lib/shared/profile.py lib/shared/catalog_writer.py tests/unit/discover/test_discover.py tests/unit/profile/test_profile.py
git commit -m "VU-1094: add seed table catalog state"
```

## Task 2: Public `add-seed-table` CLI

**Files:**

- Create: `lib/shared/cli/add_seed_table_cmd.py`
- Modify: `lib/shared/cli/main.py`
- Modify: `tests/unit/cli/test_pipeline_cmds.py`
- Modify: `repo-map.json`

- [ ] **Step 1: Add failing CLI tests**

Append near the existing `add-source-table` tests in `tests/unit/cli/test_pipeline_cmds.py`:

```python
def test_add_seed_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(fqn="silver.lookup", type="table", ready=True, reason="ok"),
    )
    write_out = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")
    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "seed     silver.lookup" in result.output
    assert "is_seed: true" in result.output


def test_add_seed_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            fqn="silver.lookup",
            type="table",
            ready=False,
            reason="object_not_found",
            code="OBJECT_NOT_FOUND",
        ),
    )
    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed") as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_write.assert_not_called()
    assert "skipped  silver.lookup" in result.output
```

Add imports:

```python
from shared.output_models.catalog_writer import WriteSeedOutput, WriteSourceOutput
```

- [ ] **Step 2: Run failing CLI tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py::test_add_seed_table_marks_valid_tables ../tests/unit/cli/test_pipeline_cmds.py::test_add_seed_table_skips_tables_that_fail_guard -q
```

Expected: fail because command/module is missing.

- [ ] **Step 3: Implement CLI command**

Create `lib/shared/cli/add_seed_table_cmd.py`:

```python
"""add-seed-table command -- add seed tables to the migration catalog."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.catalog_writer import run_write_seed
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import remind_review_and_commit, success, warn
from shared.dry_run_core import run_ready
from shared.loader_data import CatalogFileMissingError
from shared.output_models.dry_run import DryRunOutput

logger = logging.getLogger(__name__)


def add_seed_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to add"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Add one or more seed tables to the migration catalog."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()
    logger.info(
        "event=add_seed_table_start component=add_seed_table_cmd operation=add_seed_table fqns=%s",
        fqns,
    )

    written_pairs: list[tuple[str, Path]] = []

    for fqn in fqns:
        ready_result: DryRunOutput = run_ready(root, "scope", fqn)
        if ready_result.object is not None:
            is_ready = ready_result.object.ready
            reason = ready_result.object.reason
        elif ready_result.project is not None:
            is_ready = ready_result.project.ready
            reason = ready_result.project.reason
        else:
            raise AssertionError(f"run_ready returned neither object nor project payload for {fqn}")

        if not is_ready:
            warn(f"skipped  {fqn} -- {reason}")
            logger.info(
                "event=add_seed_table_skip component=add_seed_table_cmd "
                "operation=add_seed_table fqn=%s reason=%s",
                fqn,
                reason,
            )
            continue

        try:
            with cli_error_handler(f"marking {fqn} as seed table"):
                write_result = run_write_seed(root, fqn, value=True)
            success(f"seed     {fqn} -> is_seed: true")
            logger.info(
                "event=add_seed_table_written component=add_seed_table_cmd "
                "operation=add_seed_table fqn=%s written=%s status=success",
                fqn,
                write_result.written,
            )
            written_pairs.append((fqn, root / write_result.written))
        except typer.Exit:
            raise
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file -- run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} -- {exc}")

    if written_pairs:
        remind_review_and_commit()
```

Use ASCII arrows if the existing CLI output tests are sensitive to terminal encoding; otherwise follow existing command style.

In `lib/shared/cli/main.py`:

```python
from shared.cli.add_seed_table_cmd import add_seed_table
app.command("add-source-table")(add_source_table)
app.command("add-seed-table")(add_seed_table)
```

In `repo-map.json`, add:

```json
"ad_migration_add_seed_table": "cd packages/ad-migration-cli && uv run ad-migration add-seed-table <fqn>"
```

- [ ] **Step 4: Run CLI tests and commit**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py::test_add_seed_table_marks_valid_tables ../tests/unit/cli/test_pipeline_cmds.py::test_add_seed_table_skips_tables_that_fail_guard -q
```

Expected: pass.

Commit:

```bash
git add lib/shared/cli/add_seed_table_cmd.py lib/shared/cli/main.py tests/unit/cli/test_pipeline_cmds.py repo-map.json
git commit -m "VU-1094: add seed table CLI"
```

## Task 3: Readiness And Pipeline Status

**Files:**

- Modify: `lib/shared/dry_run_core.py`
- Modify: `lib/shared/pipeline_status.py`
- Test: `tests/unit/dry_run/test_dry_run.py`
- Test: `tests/unit/batch_plan/test_pipeline_status.py`

- [ ] **Step 1: Add failing readiness/status tests**

In `tests/unit/dry_run/test_dry_run.py`, add near `test_ready_source_table`:

```python
def test_ready_seed_table() -> None:
    """Seed table returns not_applicable with SEED_TABLE code."""
    tmp, root = _make_project()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_source"] = False
        cat["is_seed"] = True
        cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "seed", "source": "catalog"},
        }
        cat_path.write_text(json.dumps(cat), encoding="utf-8")
        result = dry_run.run_ready(root, "profile", object_fqn="silver.DimCustomer")
        assert isinstance(result, DryRunOutput)
        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "SEED_TABLE"
```

In `tests/unit/batch_plan/test_pipeline_status.py`, add near writerless tests:

```python
def test_table_n_a_seed(self, tmp_path):
    """Seed table returns n_a when pipeline status is queried directly."""
    dbt_root = tmp_path / "dbt"
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "catalog" / "tables" / "silver.lookup.json").write_text(
        json.dumps({
            "schema": "silver",
            "name": "Lookup",
            "is_seed": True,
            "is_source": False,
            "profile": {
                "status": "ok",
                "classification": {"resolved_kind": "seed", "source": "catalog"},
            },
        }),
        encoding="utf-8",
    )
    assert object_pipeline_status(tmp_path, "silver.lookup", "table", dbt_root) == "n_a"
```

- [ ] **Step 2: Run failing tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py::test_ready_seed_table ../tests/unit/batch_plan/test_pipeline_status.py::TestObjectPipelineStatus::test_table_n_a_seed -q
```

Expected: fail because seed tables are not special-cased.

- [ ] **Step 3: Implement readiness/status handling**

In `lib/shared/dry_run_core.py`, check seed before source in the table not-applicable block:

```python
            if cat.is_seed:
                object_detail = _object_detail(norm, obj_type, False, "not_applicable", "SEED_TABLE", not_applicable=True)
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail,
                )
            if cat.is_source:
                object_detail = _object_detail(norm, obj_type, False, "not_applicable", "SOURCE_TABLE", not_applicable=True)
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail,
                )
```

In `lib/shared/pipeline_status.py`, after gathering table diagnostics and before scoping status:

```python
    if cat.is_seed:
        return "n_a", diagnostics
```

- [ ] **Step 4: Run readiness/status tests and commit**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py::test_ready_source_table ../tests/unit/dry_run/test_dry_run.py::test_ready_seed_table ../tests/unit/dry_run/test_dry_run.py::test_ready_profile_writerless_table ../tests/unit/batch_plan/test_pipeline_status.py::TestObjectPipelineStatus::test_table_n_a_writerless ../tests/unit/batch_plan/test_pipeline_status.py::TestObjectPipelineStatus::test_table_n_a_seed -q
```

Expected: pass.

Commit:

```bash
git add lib/shared/dry_run_core.py lib/shared/pipeline_status.py tests/unit/dry_run/test_dry_run.py tests/unit/batch_plan/test_pipeline_status.py
git commit -m "VU-1094: report seed tables in readiness"
```

## Task 4: Batch Plan Seed Reporting

**Files:**

- Modify: `lib/shared/batch_plan.py`
- Modify: `lib/shared/output_models/dry_run.py`
- Test: `tests/unit/batch_plan/test_catalog_and_phases.py`
- Test: `tests/unit/batch_plan/test_scheduling.py`

- [ ] **Step 1: Add failing batch-plan tests**

In `tests/unit/batch_plan/test_catalog_and_phases.py`, add:

```python
def test_classifies_seed_tables(self, tmp_path):
    """Tables with is_seed=True go to seed_table_fqns."""
    cat_dir = tmp_path / "catalog" / "tables"
    cat_dir.mkdir(parents=True)
    (cat_dir / "dbo.seed_lookup.json").write_text(
        json.dumps({"schema": "dbo", "name": "seed_lookup", "is_seed": True}),
        encoding="utf-8",
    )
    inv = _enumerate_catalog(tmp_path)
    assert inv.seed_table_fqns == ["dbo.seed_lookup"]
    assert inv.source_table_fqns == []
    assert inv.table_fqns == []
```

Add a plan-output assertion near existing summary tests:

```python
def test_plan_output_includes_seed_tables(self, tmp_path):
    """Plan output exposes seed table count and list separately."""
    inv = _CatalogInventory(seed_table_fqns=["dbo.seed_lookup"])
    result = _build_plan_output(tmp_path, inv)
    assert result.summary.seed_tables == 1
    assert result.seed_tables[0].fqn == "dbo.seed_lookup"
    assert result.seed_tables[0].reason == "is_seed"
```

If `_CatalogInventory` construction does not accept constructor args, instantiate and assign:

```python
inv = _CatalogInventory()
inv.seed_table_fqns.append("dbo.seed_lookup")
```

- [ ] **Step 2: Run failing batch tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/batch_plan/test_catalog_and_phases.py::TestEnumerateCatalog::test_classifies_seed_tables ../tests/unit/batch_plan/test_catalog_and_phases.py::test_plan_output_includes_seed_tables -q
```

Expected: fail because output contracts have no seed fields.

- [ ] **Step 3: Extend output models and inventory**

In `lib/shared/output_models/dry_run.py`, mirror `SourceTable` with:

```python
class SeedTable(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    type: str
    reason: str
```

Add `seed_tables: int` to `BatchSummary` and `seed_tables: list[SeedTable]` to `BatchPlanOutput`.

In `lib/shared/batch_plan.py`, add `seed_table_fqns: list[str] = field(default_factory=list)`
to `_CatalogInventory` immediately after `source_table_fqns`.

In `_enumerate_catalog`, classify seed before source:

```python
            elif cat is not None and cat.is_seed:
                inv.seed_table_fqns.append(fqn)
            elif cat is not None and cat.is_source:
                inv.source_table_fqns.append(fqn)
```

In `_build_plan_output`, add `seed_tables=len(inv.seed_table_fqns)` to `BatchSummary`
construction and add this field to `BatchPlanOutput` construction:

```python
        seed_tables=[
            SeedTable(fqn=fqn, type="table", reason="is_seed")
            for fqn in sorted(inv.seed_table_fqns)
        ],
```

Update imports to include `SeedTable`.

- [ ] **Step 4: Run batch tests and commit**

Run:

```bash
cd lib && uv run pytest ../tests/unit/batch_plan/test_catalog_and_phases.py ../tests/unit/batch_plan/test_scheduling.py -q
```

Expected: pass.

Commit:

```bash
git add lib/shared/batch_plan.py lib/shared/output_models/dry_run.py tests/unit/batch_plan/test_catalog_and_phases.py tests/unit/batch_plan/test_scheduling.py
git commit -m "VU-1094: expose seed tables in batch plans"
```

## Task 5: Command And Skill Contracts

**Files:**

- Modify: `commands/profile.md`
- Modify: `skills/profiling-table/SKILL.md`
- Test: `tests/evals/packages/cmd-profile/`

- [ ] **Step 1: Update `/profile-tables` command guard**

In `commands/profile.md`, change the source-only guard to source/seed routing:

```md
- For each FQN argument:
  - if `catalog/tables/<fqn>.json` has `"is_seed": true`, record an item result with `status: "ok"`, `catalog_path`, and a seed-table warning-free outcome; print that the table is marked as a dbt seed and no writer-driven profiling is needed.
  - if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
    > `<fqn>` is marked as a dbt source -- no migration needed. Use `ad-migration add-source-table` to manage source tables.
```

Update the item result schema to include seed `ok` results without duplicating the profile payload.

- [ ] **Step 2: Update `/profiling-table` seed behavior**

In `skills/profiling-table/SKILL.md`, add a Seed Pipeline before the View/Table pipelines:

````md
## Seed Pipeline

If readiness returns `SEED_TABLE`, read the catalog entry and verify it has `is_seed: true`.
If the profile is missing or is not a seed classification, persist the canonical seed profile with:

```bash
mkdir -p .staging
cat > .staging/seed_profile.json <<'EOF'
{"classification":{"resolved_kind":"seed","source":"catalog","rationale":"Table is maintained as a dbt seed."},"warnings":[],"errors":[]}
EOF
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" profile write \
  --table <table_fqn> \
  --profile-file .staging/seed_profile.json && rm -rf .staging
```

Report that the table is seed-backed and no writer-driven profiling was applied.
````

- [ ] **Step 3: Run markdownlint and affected eval**

Run:

```bash
markdownlint commands/profile.md skills/profiling-table/SKILL.md
cd tests/evals && npm run eval:cmd-profile
```

Expected: markdownlint and promptfoo both pass. If `eval:cmd-profile` fails because a
fixture expects source-only skips, update that fixture to include distinct seed-table reporting and
rerun `npm run eval:cmd-profile` until it passes.

- [ ] **Step 4: Commit command and skill updates**

Commit:

```bash
git add commands/profile.md skills/profiling-table/SKILL.md
git commit -m "VU-1094: document seed profiling workflow"
```

If eval fixtures changed, stage the exact changed files under `tests/evals/packages/cmd-profile/`
in the same commit.

## Task 6: Full Verification And Linear Evidence

**Files:**

- No source files expected unless verification finds a defect.

- [ ] **Step 1: Run changed-area unit tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/discover/test_discover.py ../tests/unit/profile/test_profile.py ../tests/unit/cli/test_pipeline_cmds.py ../tests/unit/dry_run/test_dry_run.py ../tests/unit/batch_plan -q
```

Expected: pass.

- [ ] **Step 2: Run repo-required markdown and eval checks**

Run:

```bash
markdownlint docs/design/seed-table-catalog-state/README.md docs/design/README.md commands/profile.md skills/profiling-table/SKILL.md
cd tests/evals && npm run eval:cmd-profile
```

Expected: pass.

- [ ] **Step 3: Inspect final diff**

Run:

```bash
git diff --stat main...HEAD
git status --short
```

Expected: committed implementation changes only and no unexpected untracked files.

- [ ] **Step 4: Independent quality gates**

Dispatch independent reviewers with the issue text, design spec, this plan, commit range, changed-file list, and verification output:

- code review
- simplification review
- test coverage review
- acceptance-criteria review

Resolve every blocking finding before continuing. Treat unproven acceptance criteria or critical missing tests as failing gates.

- [ ] **Step 5: Update Linear and final implementation commit**

Update the VU-1094 main issue description by checking off only acceptance criteria proven by tests and reviewers. Add an implementation comment with:

- what was implemented
- test, eval, and markdownlint commands run
- independent-review outcomes
- remaining risks or blocked checks

If final fixes were needed after review, stage the exact files changed by those fixes and commit:

```bash
git add lib/shared/catalog_models.py lib/shared/output_models/writeback.py lib/shared/output_models/catalog_writer.py lib/shared/profile.py lib/shared/catalog_writer.py lib/shared/cli/add_source_table_cmd.py lib/shared/cli/add_seed_table_cmd.py lib/shared/cli/main.py lib/shared/dry_run_core.py lib/shared/pipeline_status.py lib/shared/batch_plan.py lib/shared/output_models/dry_run.py commands/profile.md skills/profiling-table/SKILL.md repo-map.json tests/unit/discover/test_discover.py tests/unit/profile/test_profile.py tests/unit/cli/test_pipeline_cmds.py tests/unit/dry_run/test_dry_run.py tests/unit/batch_plan/test_pipeline_status.py tests/unit/batch_plan/test_catalog_and_phases.py tests/unit/batch_plan/test_scheduling.py
git commit -m "VU-1094: finalize seed table profiling"
```

End with a clean worktree:

```bash
git status --short
```

Expected: no output.
