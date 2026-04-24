# Python Monolith Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the highest-risk Python monoliths found in code review: sandbox service ownership, migrate-util readiness branching, and profile catalog contract validation.

**Architecture:** Keep public APIs stable and move behavior behind smaller units that already match the repo's current direction. Land each cleanup as a separate, tested commit: sandbox service ownership first, readiness policy dispatch second, profile model tightening third.

**Tech Stack:** Python 3.11, Pydantic v2, Typer, pytest, existing `shared` package patterns.

---

## File Structure

- Modify: `lib/shared/sandbox/sql_server.py`
  Public facade remains the import point for `SqlServerSandbox`.
- Modify: `lib/shared/sandbox/sql_server_services.py`
  Reduce to SQL Server shared helpers, connection/config core, and compatibility exports only.
- Modify: `lib/shared/sandbox/sql_server_lifecycle.py`
  Own SQL Server sandbox create/drop/status/clone behavior.
- Modify: `lib/shared/sandbox/sql_server_fixtures.py`
  Own SQL Server fixture seeding and view materialization behavior.
- Modify: `lib/shared/sandbox/sql_server_execution.py`
  Own SQL Server scenario and read-only select execution.
- Modify: `lib/shared/sandbox/sql_server_comparison.py`
  Own SQL Server read-only SQL comparison.
- Modify: `lib/shared/sandbox/oracle.py`
  Public facade remains the import point for `OracleSandbox`.
- Modify: `lib/shared/sandbox/oracle_services.py`
  Reduce to Oracle shared helpers, connection/config core, and compatibility exports only.
- Modify: `lib/shared/sandbox/oracle_lifecycle.py`
  Own Oracle sandbox create/drop/status/clone behavior.
- Modify: `lib/shared/sandbox/oracle_fixtures.py`
  Own Oracle fixture seeding and view materialization behavior.
- Modify: `lib/shared/sandbox/oracle_execution.py`
  Own Oracle scenario and read-only select execution.
- Modify: `lib/shared/sandbox/oracle_comparison.py`
  Own Oracle read-only SQL comparison.
- Modify: `tests/unit/test_harness/test_sandbox_services.py`
  Add boundary tests that prove service methods no longer delegate straight back to core monolith methods.
- Modify: `lib/shared/dry_run_support/readiness.py`
  Split object context loading, applicability checks, and per-stage readiness functions.
- Modify: `tests/unit/dry_run/test_dry_run.py`
  Add focused tests for stage dispatch and applicability behavior.
- Modify: `lib/shared/catalog_models.py`
  Replace permissive profile `Any` fields with typed nested Pydantic models and `Literal` enums.
- Modify: `lib/shared/profile.py`
  Replace manual profile enum validation with model validation and status derivation helpers.
- Modify: `tests/unit/profile/test_profile.py`
  Update invalid/valid profile tests to assert model-driven validation.
- Optional Modify: `repo-map.json`
  Update only if module responsibilities change enough that the existing sandbox/profile/readiness descriptions become stale.

## Task 1: Add Boundary Tests for Real Sandbox Service Ownership

**Files:**

- Modify: `tests/unit/test_harness/test_sandbox_services.py`

- [ ] **Step 1: Write failing boundary tests**

Add these imports:

```python
import inspect
```

Add these helpers and tests at the end of `tests/unit/test_harness/test_sandbox_services.py`:

```python
def _method_source(cls: type[object], method_name: str) -> str:
    return inspect.getsource(getattr(cls, method_name))


def test_sql_server_services_own_behavior_instead_of_core_delegation() -> None:
    delegated_methods = {
        SqlServerLifecycleService: [
            "sandbox_up",
            "sandbox_reset",
            "sandbox_down",
            "sandbox_status",
        ],
        SqlServerExecutionService: [
            "execute_scenario",
            "execute_select",
        ],
        SqlServerComparisonService: [
            "compare_two_sql",
        ],
    }

    for cls, method_names in delegated_methods.items():
        for method_name in method_names:
            source = _method_source(cls, method_name)
            assert "_SqlServerSandboxCore." not in source


def test_oracle_services_own_behavior_instead_of_core_delegation() -> None:
    delegated_methods = {
        OracleLifecycleService: [
            "sandbox_up",
            "sandbox_reset",
            "sandbox_down",
            "sandbox_status",
        ],
        OracleExecutionService: [
            "execute_scenario",
            "execute_select",
        ],
        OracleComparisonService: [
            "compare_two_sql",
        ],
    }

    for cls, method_names in delegated_methods.items():
        for method_name in method_names:
            source = _method_source(cls, method_name)
            assert "_OracleSandboxCore." not in source
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
cd lib && uv run pytest ../tests/unit/test_harness/test_sandbox_services.py -q
```

Expected: FAIL because service methods currently call `_SqlServerSandboxCore.*` and `_OracleSandboxCore.*` directly.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/unit/test_harness/test_sandbox_services.py
git commit -m "test: lock sandbox service ownership boundary"
```

## Task 2: Move SQL Server Sandbox Behavior Into Real Services

**Files:**

- Modify: `lib/shared/sandbox/sql_server_services.py`
- Modify: `lib/shared/sandbox/sql_server_lifecycle.py`
- Modify: `lib/shared/sandbox/sql_server_fixtures.py`
- Modify: `lib/shared/sandbox/sql_server_execution.py`
- Modify: `lib/shared/sandbox/sql_server_comparison.py`
- Modify: `tests/unit/test_harness/test_sqlserver_sandbox.py`
- Modify: `tests/unit/test_harness/test_views_and_compare.py`

- [ ] **Step 1: Move lifecycle methods**

Move these methods from `_SqlServerSandboxCore` in `sql_server_services.py` into `SqlServerLifecycleService` in `sql_server_lifecycle.py`:

```text
_create_sandbox_db
_create_schemas
_load_object_columns
_render_column_type
_create_empty_table
_clone_tables
_clone_views
_clone_procedures
_sandbox_clone_into
sandbox_up
sandbox_reset
sandbox_down
sandbox_status
_sandbox_content_counts
```

Inside moved methods, replace `self.` calls that need backend connection/config state with `self._backend.`. Keep helper calls such as `_validate_identifier`, `_validate_sandbox_db_name`, `_import_pyodbc`, `generate_sandbox_name`, and output model constructors imported directly into `sql_server_lifecycle.py`.

For example, the moved `sandbox_up` method should have this shape:

```python
def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
    sandbox_db = generate_sandbox_name()
    logger.info(
        "event=sandbox_up sandbox_db=%s source=%s schemas=%s",
        sandbox_db,
        self._backend.source_database,
        schemas,
    )
    result = self._sandbox_clone_into(sandbox_db, schemas)
    logger.info(
        "event=sandbox_up_complete sandbox_db=%s status=%s "
        "tables=%d views=%d procedures=%d errors=%d",
        sandbox_db,
        result.status,
        len(result.tables_cloned),
        len(result.views_cloned),
        len(result.procedures_cloned),
        len(result.errors),
    )
    return result
```

- [ ] **Step 2: Move fixture methods**

Move these methods from `_SqlServerSandboxCore` into `SqlServerFixtureService`:

```text
_seed_fixtures
_ensure_view_tables
```

Keep the public service methods as thin names:

```python
def seed_fixtures(
    self,
    cursor: Any,
    sandbox_db: str,
    fixtures: list[dict[str, Any]],
) -> None:
    self._seed_fixtures(cursor, sandbox_db, fixtures)


def ensure_view_tables(
    self,
    sandbox_db: str,
    given: list[dict[str, Any]],
) -> list[str]:
    return self._ensure_view_tables(sandbox_db, given)
```

Inside moved implementation, replace calls to `_load_object_columns` and `_create_empty_table` with the lifecycle service:

```python
columns = self._backend._lifecycle.load_object_columns(src_cur, schema_name, obj_name)
self._backend._lifecycle.create_empty_table(sb_cur, schema_name, obj_name, columns)
```

Expose those two lifecycle helpers without leading underscores if fixture code needs them:

```python
def load_object_columns(...): ...
def create_empty_table(...): ...
```

- [ ] **Step 3: Move execution methods**

Move `execute_scenario` and `execute_select` into `SqlServerExecutionService`.

Replace fixture calls with:

```python
self._backend._fixtures.ensure_view_tables(sandbox_db, given)
self._backend._fixtures.seed_fixtures(cursor, sandbox_db, given)
```

For `execute_select`, use:

```python
self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
self._backend._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)
```

- [ ] **Step 4: Move comparison method**

Move `compare_two_sql` into `SqlServerComparisonService`.

Replace fixture calls with:

```python
self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
self._backend._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)
```

- [ ] **Step 5: Keep compatibility hooks for existing tests**

In `SqlServerSandbox`, keep test-friendly methods that forward to services because existing tests patch private methods:

```python
def _seed_fixtures(
    self,
    cursor: Any,
    sandbox_db: str,
    fixtures: list[dict[str, Any]],
) -> None:
    self._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)


def _ensure_view_tables(
    self,
    sandbox_db: str,
    given: list[dict[str, Any]],
) -> list[str]:
    return self._fixtures.ensure_view_tables(sandbox_db, given)
```

Only keep these if the existing tests still patch them. Prefer updating tests to patch service methods after the public behavior is stable.

- [ ] **Step 6: Remove moved methods from core**

After tests pass for SQL Server, delete the moved implementation methods from `_SqlServerSandboxCore`. Keep only constructor, `from_env`, `_connect`, `_connect_source`, and shared helper exports in `sql_server_services.py`.

- [ ] **Step 7: Run targeted SQL Server tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/test_harness/test_sandbox_services.py ../tests/unit/test_harness/test_sqlserver_sandbox.py ../tests/unit/test_harness/test_views_and_compare.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit SQL Server service extraction**

```bash
git add lib/shared/sandbox/sql_server.py lib/shared/sandbox/sql_server_services.py lib/shared/sandbox/sql_server_lifecycle.py lib/shared/sandbox/sql_server_fixtures.py lib/shared/sandbox/sql_server_execution.py lib/shared/sandbox/sql_server_comparison.py tests/unit/test_harness/test_sqlserver_sandbox.py tests/unit/test_harness/test_views_and_compare.py tests/unit/test_harness/test_sandbox_services.py
git commit -m "refactor: move sql server sandbox behavior into services"
```

## Task 3: Move Oracle Sandbox Behavior Into Real Services

**Files:**

- Modify: `lib/shared/sandbox/oracle_services.py`
- Modify: `lib/shared/sandbox/oracle_lifecycle.py`
- Modify: `lib/shared/sandbox/oracle_fixtures.py`
- Modify: `lib/shared/sandbox/oracle_execution.py`
- Modify: `lib/shared/sandbox/oracle_comparison.py`
- Modify: `tests/unit/test_harness/test_oracle_sandbox.py`
- Modify: `tests/unit/test_harness/test_views_and_compare.py`

- [ ] **Step 1: Move lifecycle methods**

Move these methods from `_OracleSandboxCore` into `OracleLifecycleService`:

```text
_create_sandbox_schema
_load_object_columns
_render_column_type
_create_empty_table
_clone_tables
_clone_views
_clone_procedures
_sandbox_clone_into
sandbox_up
sandbox_reset
sandbox_down
sandbox_status
_sandbox_content_counts
```

Inside moved methods, use `self._backend` for connection and config state. Import Oracle helper functions and output models directly in `oracle_lifecycle.py`.

- [ ] **Step 2: Move fixture methods**

Move `_seed_fixtures` and `_ensure_view_tables` into `OracleFixtureService`.

Keep these public service wrappers:

```python
def seed_fixtures(
    self,
    cursor: Any,
    sandbox_schema: str,
    fixtures: list[dict[str, Any]],
) -> None:
    self._seed_fixtures(cursor, sandbox_schema, fixtures)


def ensure_view_tables(
    self,
    sandbox_db: str,
    given: list[dict[str, Any]],
) -> list[str]:
    return self._ensure_view_tables(sandbox_db, given)
```

Use lifecycle service helpers for column loading and shell-table creation:

```python
columns = self._backend._lifecycle.load_object_columns(
    source_cursor,
    self._backend.source_schema,
    view_name,
)
self._backend._lifecycle.create_empty_table(
    sandbox_cursor,
    sandbox_db,
    view_name,
    columns,
)
```

- [ ] **Step 3: Move execution methods**

Move `execute_scenario` and `execute_select` into `OracleExecutionService`.

Use:

```python
self._backend._fixtures.ensure_view_tables(sandbox_db, given)
self._backend._fixtures.seed_fixtures(cursor, sandbox_db, given)
```

- [ ] **Step 4: Move comparison method**

Move `compare_two_sql` into `OracleComparisonService`.

Use:

```python
self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
self._backend._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)
```

- [ ] **Step 5: Keep compatibility hooks for existing tests**

In `OracleSandbox`, keep:

```python
def _seed_fixtures(
    self,
    cursor: Any,
    sandbox_schema: str,
    fixtures: list[dict[str, Any]],
) -> None:
    self._fixtures.seed_fixtures(cursor, sandbox_schema, fixtures)


def _ensure_view_tables(
    self,
    sandbox_db: str,
    given: list[dict[str, Any]],
) -> list[str]:
    return self._fixtures.ensure_view_tables(sandbox_db, given)
```

- [ ] **Step 6: Remove moved methods from core**

After targeted tests pass, delete moved implementation methods from `_OracleSandboxCore`. Keep only constructor, `from_env`, `_connect`, `_connect_source`, and helper exports in `oracle_services.py`.

- [ ] **Step 7: Run targeted Oracle tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/test_harness/test_sandbox_services.py ../tests/unit/test_harness/test_oracle_sandbox.py ../tests/unit/test_harness/test_views_and_compare.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit Oracle service extraction**

```bash
git add lib/shared/sandbox/oracle.py lib/shared/sandbox/oracle_services.py lib/shared/sandbox/oracle_lifecycle.py lib/shared/sandbox/oracle_fixtures.py lib/shared/sandbox/oracle_execution.py lib/shared/sandbox/oracle_comparison.py tests/unit/test_harness/test_oracle_sandbox.py tests/unit/test_harness/test_views_and_compare.py tests/unit/test_harness/test_sandbox_services.py
git commit -m "refactor: move oracle sandbox behavior into services"
```

## Task 4: Split Readiness Into Context, Applicability, and Stage Dispatch

**Files:**

- Modify: `lib/shared/dry_run_support/readiness.py`
- Modify: `tests/unit/dry_run/test_dry_run.py`

- [ ] **Step 1: Add tests for dispatchable stage rules**

Add this test near the existing invalid-stage readiness tests:

```python
def test_ready_invalid_stage_does_not_load_object_catalog() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "manifest.json").write_text("{}", encoding="utf-8")

        result = dry_run.run_ready(root, "bogus", object_fqn="silver.DimCustomer")

        assert result.ready is False
        assert result.project.reason == "invalid_stage"
        assert result.object is None
```

Add this test near excluded/source/seed readiness tests:

```python
def test_ready_excluded_table_short_circuits_before_stage_policy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        catalog_dir = root / "catalog" / "tables"
        catalog_dir.mkdir(parents=True)
        (root / "manifest.json").write_text("{}", encoding="utf-8")
        (catalog_dir / "silver.dimcustomer.json").write_text(
            json.dumps({
                "schema": "silver",
                "name": "DimCustomer",
                "excluded": True,
            }),
            encoding="utf-8",
        )

        result = dry_run.run_ready(root, "generate", object_fqn="silver.DimCustomer")

        assert result.ready is False
        assert result.object is not None
        assert result.object.reason == "not_applicable"
        assert result.object.code == "EXCLUDED"
        assert result.object.not_applicable is True
```

- [ ] **Step 2: Run the focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -q
```

Expected before refactor: the new invalid-stage test should fail if object catalog work happens before project stage validation; if it already passes, keep it as a regression test.

- [ ] **Step 3: Introduce object context**

Add this dataclass to `readiness.py`:

```python
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class _ObjectReadinessContext:
    fqn: str
    obj_type: str
    catalog: Any | None
```

Add this loader:

```python
def _load_object_context(project_root: Path, object_fqn: str) -> _ObjectReadinessContext | ReadinessDetail:
    norm = normalize(object_fqn)
    obj_type = detect_object_type(project_root, norm)
    if obj_type is None:
        return object_detail(
            norm,
            None,
            False,
            "object_not_found",
            "OBJECT_NOT_FOUND",
        )

    try:
        if obj_type == "table":
            catalog = load_table_catalog(project_root, norm)
        else:
            catalog = load_view_catalog(project_root, norm)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        catalog = None

    return _ObjectReadinessContext(fqn=norm, obj_type=obj_type, catalog=catalog)
```

- [ ] **Step 4: Extract applicability checks**

Add:

```python
def _not_applicable_output(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
    code: str,
) -> DryRunOutput:
    entry = object_detail(
        ctx.fqn,
        ctx.obj_type,
        False,
        "not_applicable",
        code,
        not_applicable=True,
    )
    return DryRunOutput(stage=stage, ready=False, project=project, object=entry)


def _object_applicability(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput | None:
    cat = ctx.catalog
    if ctx.obj_type == "table" and cat is not None:
        if cat.is_seed:
            return _not_applicable_output(stage=stage, project=project, ctx=ctx, code="SEED_TABLE")
        if cat.is_source:
            return _not_applicable_output(stage=stage, project=project, ctx=ctx, code="SOURCE_TABLE")
        if cat.excluded:
            return _not_applicable_output(stage=stage, project=project, ctx=ctx, code="EXCLUDED")
    if ctx.obj_type in ("view", "mv") and cat is not None and cat.excluded:
        return _not_applicable_output(stage=stage, project=project, ctx=ctx, code="EXCLUDED")
    return None
```

- [ ] **Step 5: Extract per-stage object rules**

Add:

```python
def _object_out(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
    ready: bool,
    reason: str,
    code: str | None = None,
) -> DryRunOutput:
    entry = object_detail(ctx.fqn, ctx.obj_type, ready, reason, code)
    return DryRunOutput(stage=stage, ready=ready and project.ready, project=project, object=entry)


def _scope_ready(project_root: Path, stage: str, project: ReadinessDetail, ctx: _ObjectReadinessContext) -> DryRunOutput:
    if detect_catalog_bucket(project_root, ctx.fqn) is None:
        return _object_out(stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing")
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def _profile_ready(project_root: Path, stage: str, project: ReadinessDetail, ctx: _ObjectReadinessContext) -> DryRunOutput:
    cat = ctx.catalog
    if cat is None:
        return _object_out(stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing")
    scoping_status = cat.scoping.status if cat.scoping else None
    if ctx.obj_type in ("view", "mv"):
        if scoping_status != "analyzed":
            return _object_out(stage=stage, project=project, ctx=ctx, ready=False, reason="scoping_not_analyzed")
        return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
    if scoping_status == "no_writer_found":
        return _not_applicable_output(stage=stage, project=project, ctx=ctx, code="WRITERLESS_TABLE")
    if scoping_status != "resolved":
        return _object_out(stage=stage, project=project, ctx=ctx, ready=False, reason="scoping_not_resolved")
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
```

Also extract `_test_gen_ready`, `_refactor_ready`, and `_generate_ready` from the existing branches without changing reason strings.

- [ ] **Step 6: Dispatch from `run_ready`**

Use this structure:

```python
_STAGE_OBJECT_CHECKS: dict[str, Callable[[Path, str, ReadinessDetail, _ObjectReadinessContext], DryRunOutput]] = {
    "scope": _scope_ready,
    "profile": _profile_ready,
    "test-gen": _test_gen_ready,
    "refactor": _refactor_ready,
    "generate": _generate_ready,
}


def run_ready(project_root: Path, stage: str, object_fqn: str | None = None) -> DryRunOutput:
    if stage not in VALID_STAGES:
        return DryRunOutput(stage=stage, ready=False, project=detail(False, "invalid_stage"))

    project = _project_stage_ready(project_root, stage)
    if not project.ready:
        return DryRunOutput(stage=stage, ready=False, project=project)

    if object_fqn is None:
        return DryRunOutput(stage=stage, ready=True, project=project)

    ctx_or_error = _load_object_context(project_root, object_fqn)
    if isinstance(ctx_or_error, ReadinessDetail):
        return DryRunOutput(stage=stage, ready=False, project=project, object=ctx_or_error)
    ctx = ctx_or_error

    not_applicable = _object_applicability(stage=stage, project=project, ctx=ctx)
    if not_applicable is not None:
        return not_applicable

    checker = _STAGE_OBJECT_CHECKS.get(stage)
    if checker is None:
        return _object_out(stage=stage, project=project, ctx=ctx, ready=False, reason="invalid_stage")
    return checker(project_root, stage, project, ctx)
```

- [ ] **Step 7: Run dry-run tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/dry_run/test_dry_run.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit readiness split**

```bash
git add lib/shared/dry_run_support/readiness.py tests/unit/dry_run/test_dry_run.py
git commit -m "refactor: split readiness stage policy"
```

## Task 5: Tighten Profile Catalog Contracts With Typed Models

**Files:**

- Modify: `lib/shared/catalog_models.py`
- Modify: `lib/shared/profile.py`
- Modify: `tests/unit/profile/test_profile.py`

- [ ] **Step 1: Add model-driven validation tests**

Add or update invalid enum tests in `tests/unit/profile/test_profile.py` so they assert the model contract raises through `run_write`. Example:

```python
def test_write_profile_invalid_classification_rejected_by_model() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_catalog(root, "silver", "FactSales")
        bad_profile = copy.deepcopy(_VALID_PROFILE)
        bad_profile["classification"]["resolved_kind"] = "not_real"

        with pytest.raises(ValidationError):
            profile.run_write(root, "silver.FactSales", bad_profile)
```

Add equivalent tests for:

```text
primary_key.primary_key_type
foreign_keys[0].fk_type
pii_actions[0].suggested_action
view profile classification
view profile source
```

If current tests expect `ValueError`, update them to expect `ValidationError` once the model owns the contract.

- [ ] **Step 2: Run profile tests and verify failures**

Run:

```bash
cd lib && uv run pytest ../tests/unit/profile/test_profile.py -q
```

Expected: FAIL until catalog profile models are typed and `profile.py` stops converting model errors to manual `ValueError`.

- [ ] **Step 3: Add typed table profile models**

In `catalog_models.py`, add imports:

```python
from typing import Literal
```

Add nested models before `TableProfileSection`:

```python
ProfileResolvedKind = Literal[
    "seed",
    "dim_non_scd",
    "dim_scd1",
    "dim_scd2",
    "dim_junk",
    "fact_transaction",
    "fact_periodic_snapshot",
    "fact_accumulating_snapshot",
    "fact_aggregate",
]
ProfileSource = Literal["catalog", "llm", "catalog+llm"]
PrimaryKeyType = Literal["surrogate", "natural", "composite", "unknown"]
ForeignKeyType = Literal["standard", "role_playing", "degenerate"]
PiiSuggestedAction = Literal["mask", "drop", "tokenize", "keep"]


class TableClassificationProfile(BaseModel):
    model_config = _STRICT_CONFIG

    resolved_kind: ProfileResolvedKind
    source: ProfileSource | None = None
    rationale: str | None = None


class TablePrimaryKeyProfile(BaseModel):
    model_config = _STRICT_CONFIG

    columns: list[str] = []
    primary_key_type: PrimaryKeyType | None = None
    source: ProfileSource | None = None
    rationale: str | None = None


class TableNaturalKeyProfile(BaseModel):
    model_config = _STRICT_CONFIG

    columns: list[str] = []
    source: ProfileSource | None = None
    rationale: str | None = None


class TableWatermarkProfile(BaseModel):
    model_config = _STRICT_CONFIG

    column: str | None = None
    source: ProfileSource | None = None
    rationale: str | None = None


class TableForeignKeyProfile(BaseModel):
    model_config = _STRICT_CONFIG

    columns: list[str] = []
    referenced_table: str | None = None
    referenced_columns: list[str] = []
    fk_type: ForeignKeyType | None = None
    source: ProfileSource | None = None
    rationale: str | None = None


class TablePiiActionProfile(BaseModel):
    model_config = _STRICT_CONFIG

    column: str
    suggested_action: PiiSuggestedAction | None = None
    source: ProfileSource | None = None
    rationale: str | None = None
```

Then replace `TableProfileSection` fields:

```python
class TableProfileSection(BaseModel):
    """Profiling results for a table (classification, keys, PII, etc.)."""

    model_config = _STRICT_CONFIG

    status: Literal["", "ok", "partial", "error"] = ""
    classification: TableClassificationProfile | None = None
    primary_key: TablePrimaryKeyProfile | None = None
    natural_key: TableNaturalKeyProfile | None = None
    watermark: TableWatermarkProfile | None = None
    foreign_keys: list[TableForeignKeyProfile] = []
    pii_actions: list[TablePiiActionProfile] = []
    warnings: list[Any] = []
    errors: list[Any] = []
```

- [ ] **Step 4: Tighten view profile model**

Replace `ViewProfileSection` with:

```python
class ViewProfileSection(BaseModel):
    """Profiling results for a view (stg/mart classification)."""

    model_config = _STRICT_CONFIG

    status: Literal["", "ok", "partial", "error"] = ""
    classification: Literal["stg", "mart"]
    rationale: str
    source: Literal["llm"]
    warnings: list[Any] = []
    errors: list[Any] = []
```

- [ ] **Step 5: Replace manual validation in profile.py**

Remove these constants if no longer used:

```text
RESOLVED_KINDS
FK_TYPES
SUGGESTED_ACTIONS
SOURCES
PROFILE_STATUSES
PK_TYPES
VIEW_CLASSIFICATIONS
VIEW_SOURCES
```

Delete `_validate_profile` and `_validate_view_profile`.

Add status helpers:

```python
def _derive_table_profile_status(existing_table: TableCatalog, section: TableProfileSection) -> str:
    resolved_kind = section.classification.resolved_kind if section.classification else None
    if existing_table.is_seed and resolved_kind != "seed":
        raise ValueError(f"seed table profiles must use seed classification for {existing_table.schema}.{existing_table.name}")
    if resolved_kind == "seed" and not existing_table.is_seed:
        raise ValueError(f"seed classification requires is_seed: true for {existing_table.schema}.{existing_table.name}")
    if resolved_kind == "seed":
        return "ok"
    if section.classification is not None and section.primary_key is not None:
        return "ok"
    if section.classification is not None:
        return "partial"
    return "error"


def _derive_view_profile_status(section: ViewProfileSection) -> str:
    return "ok" if section.classification in {"stg", "mart"} else "partial"
```

In `_write_view_profile`, replace manual validation with:

```python
section = ViewProfileSection.model_validate({
    **profile_json,
    "status": profile_json.get("status", ""),
})
profile_json = section.model_dump(mode="json", exclude_none=True)
profile_json["status"] = _derive_view_profile_status(section)
```

In `run_write`, replace table manual validation with:

```python
section = TableProfileSection.model_validate({
    **profile_json,
    "status": profile_json.get("status", ""),
})
status = _derive_table_profile_status(existing_table, section)
profile_json = section.model_dump(mode="json", exclude_none=True)
profile_json["status"] = status
```

Keep the existing guard:

```python
if "status" in profile_json:
    raise ValueError("status must not be passed — determined by CLI")
```

- [ ] **Step 6: Run profile tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/profile/test_profile.py ../tests/unit/catalog_models -q
```

Expected: PASS.

- [ ] **Step 7: Commit profile contract tightening**

```bash
git add lib/shared/catalog_models.py lib/shared/profile.py tests/unit/profile/test_profile.py
git commit -m "refactor: tighten profile catalog contracts"
```

## Task 6: Final Verification and Repo Map Check

**Files:**

- Optional Modify: `repo-map.json`

- [ ] **Step 1: Check whether repo-map became stale**

Run:

```bash
rg "sandbox_backends|profile.py|dry_run_support" repo-map.json
```

If the descriptions still say the sandbox service delegates hold core behavior, update them. If descriptions remain accurate, do not edit `repo-map.json`.

- [ ] **Step 2: Run targeted unit suites**

Run:

```bash
cd lib && uv run pytest ../tests/unit/test_harness ../tests/unit/dry_run ../tests/unit/profile ../tests/unit/catalog_models -q
```

Expected: PASS.

- [ ] **Step 3: Run broader shared-library tests if time allows**

Run:

```bash
cd lib && uv run pytest
```

Expected: PASS. If local infrastructure or environment blocks integration-marked tests, record the exact skipped or failing command in the PR notes.

- [ ] **Step 4: Commit repo-map update if needed**

Only run this if `repo-map.json` changed:

```bash
git add repo-map.json
git commit -m "docs: update python module map"
```

- [ ] **Step 5: Inspect final branch history**

Run:

```bash
git status --short
git log --oneline --max-count=8
```

Expected: clean working tree after commits, with staged commits for tests, SQL Server sandbox, Oracle sandbox, readiness, profile contracts, and optional repo-map update.

## Self-Review

- Spec coverage: findings 1, 2, and 3 from the code review are covered by sandbox service ownership tasks, readiness policy dispatch, and typed profile contracts.
- Placeholder scan: no task depends on "TBD" behavior; each task names concrete files, commands, and expected outcomes.
- Type consistency: service, helper, and model names match the existing codebase names unless explicitly introduced in the task.

Plan complete. Recommended execution mode: Subagent-Driven for Tasks 2 and 3 only if the user explicitly authorizes subagents; otherwise execute inline with commits after every task.
