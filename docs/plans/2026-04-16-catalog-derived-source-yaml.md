# Catalog-Derived Source YAML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate richer dbt `sources.yml` entries for confirmed source tables from normalized table catalog metadata.

**Architecture:** Keep `setup-target` behavior routed through `shared.generate_sources`. Build source table YAML from included catalog payloads, derive deterministic tests from normalized constraint fields, and skip unresolved or composite cases rather than inferring semantics.

**Tech Stack:** Python 3.11, Pydantic output contracts, PyYAML, pytest, dbt source YAML conventions.

---

## File Structure

- Modify `lib/shared/generate_sources.py`: collect included source catalog payloads, build enriched table YAML, and keep write behavior idempotent.
- Modify `tests/unit/generate_sources/test_generate_sources.py`: add unit tests for columns, column tests, relationships, freshness, composite constraints, and written YAML.
- Read `tests/unit/target_setup/test_target_setup.py`: confirm no setup-target test needs behavior changes because target setup delegates to `write_sources_yml`.
- Do not modify `lib/shared/target_setup.py` unless tests prove setup-target needs a direct integration assertion.

## Task 1: Catalog Columns and Not-Null Tests

**Files:**

- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Modify: `lib/shared/generate_sources.py`

- [ ] **Step 1: Add failing coverage for emitted columns, data types, and not-null tests**

Append this test near the `sources.yml content` section in `tests/unit/generate_sources/test_generate_sources.py`:

```python
def test_sources_yml_includes_catalog_columns_types_and_not_null_tests() -> None:
    """Confirmed source tables emit catalog columns with type metadata and not_null tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "email", "data_type": "NVARCHAR(255)", "is_nullable": True},
                {"name": "status", "type": "VARCHAR(20)", "is_nullable": False},
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert table["columns"] == [
            {"name": "customer_id", "data_type": "INT", "tests": ["not_null"]},
            {"name": "email", "data_type": "NVARCHAR(255)"},
            {"name": "status", "data_type": "VARCHAR(20)", "tests": ["not_null"]},
        ]
    finally:
        tmp.cleanup()
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_includes_catalog_columns_types_and_not_null_tests -q
```

Expected: FAIL because generated table entries do not include `columns`.

- [ ] **Step 3: Implement catalog-column emission**

In `lib/shared/generate_sources.py`, replace the `sources_by_schema: dict[str, list[str]]` accumulator with a catalog-aware mapping and add helpers above `generate_sources`:

```python
def _column_data_type(column: dict[str, Any]) -> str | None:
    value = column.get("sql_type") or column.get("data_type") or column.get("type")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _append_test(tests: list[Any], test: Any) -> None:
    if test not in tests:
        tests.append(test)


def _build_source_columns(cat: dict[str, Any]) -> list[dict[str, Any]]:
    columns: list[dict[str, Any]] = []
    for column in cat.get("columns", []):
        name = column.get("name")
        if not name:
            continue
        entry: dict[str, Any] = {"name": str(name)}
        data_type = _column_data_type(column)
        if data_type:
            entry["data_type"] = data_type
        tests: list[Any] = []
        if column.get("is_nullable") is False:
            _append_test(tests, "not_null")
        if tests:
            entry["tests"] = tests
        columns.append(entry)
    return columns
```

Change the included-table collection so each schema stores full catalog dicts:

```python
    sources_by_schema: dict[str, list[dict[str, Any]]] = {}
```

When `is_source` is true, append the catalog:

```python
            included.append(fqn)
            sources_by_schema.setdefault(schema, []).append(cat)
```

Build table entries with columns:

```python
        tables = []
        for cat in sorted(sources_by_schema[schema_name], key=lambda item: str(item.get("name", "")).lower()):
            table_name = str(cat.get("name", ""))
            table_entry: dict[str, Any] = {
                "name": table_name,
                "description": f"{table_name} from source system",
            }
            columns = _build_source_columns(cat)
            if columns:
                table_entry["columns"] = columns
            tables.append(table_entry)
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add lib/shared/generate_sources.py tests/unit/generate_sources/test_generate_sources.py
git commit -m "VU-1099: emit source catalog columns"
```

## Task 2: Primary Key and Unique Index Tests

**Files:**

- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Modify: `lib/shared/generate_sources.py`

- [ ] **Step 1: Add failing tests for single-column and composite uniqueness**

Append these tests:

```python
def test_sources_yml_adds_unique_for_single_column_pk_and_unique_index() -> None:
    """Single-column primary keys and unique indexes emit unique tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "email", "sql_type": "NVARCHAR(255)", "is_nullable": True},
            ],
            "primary_keys": [{"constraint_name": "PK_Customer", "columns": ["customer_id"]}],
            "unique_indexes": [{"index_name": "UQ_Customer_Email", "columns": ["email"]}],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null", "unique"]
        assert columns[1]["tests"] == ["unique"]
    finally:
        tmp.cleanup()


def test_sources_yml_does_not_mark_composite_keys_individually_unique() -> None:
    """Composite primary keys and unique indexes do not add per-column unique tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "OrderLine",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                {"name": "line_id", "sql_type": "INT", "is_nullable": False},
                {"name": "sku", "sql_type": "VARCHAR(30)", "is_nullable": True},
            ],
            "primary_keys": [{"constraint_name": "PK_OrderLine", "columns": ["order_id", "line_id"]}],
            "unique_indexes": [{"index_name": "UQ_OrderLine", "columns": ["order_id", "sku"]}],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null"]
        assert columns[1]["tests"] == ["not_null"]
        assert "tests" not in columns[2]
    finally:
        tmp.cleanup()
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_adds_unique_for_single_column_pk_and_unique_index \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_does_not_mark_composite_keys_individually_unique \
  -q
```

Expected: first test FAIL because `unique` is missing.

- [ ] **Step 3: Implement deterministic single-column uniqueness**

Add this helper:

```python
def _single_column_constraint_columns(constraints: list[Any]) -> set[str]:
    columns: set[str] = set()
    for constraint in constraints:
        if not isinstance(constraint, dict):
            continue
        constraint_columns = constraint.get("columns")
        if not isinstance(constraint_columns, list) or len(constraint_columns) != 1:
            continue
        column = str(constraint_columns[0]).strip()
        if column:
            columns.add(column.lower())
    return columns
```

In `_build_source_columns`, compute unique-capable columns before iterating:

```python
    unique_columns = _single_column_constraint_columns(cat.get("primary_keys", []))
    unique_columns.update(_single_column_constraint_columns(cat.get("unique_indexes", [])))
```

Then add `unique` after the `not_null` branch:

```python
        if str(name).lower() in unique_columns:
            _append_test(tests, "unique")
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add lib/shared/generate_sources.py tests/unit/generate_sources/test_generate_sources.py
git commit -m "VU-1099: derive source uniqueness tests"
```

## Task 3: Source-Local Foreign Key Relationships

**Files:**

- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Modify: `lib/shared/generate_sources.py`

- [ ] **Step 1: Add failing tests for safe and skipped relationships**

Append these tests:

```python
def test_sources_yml_adds_relationship_for_confirmed_source_reference() -> None:
    """Single-column FKs to another confirmed source emit dbt relationships tests."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
        },
        {
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "foreign_keys": [
                {
                    "constraint_name": "FK_Order_Customer",
                    "columns": ["customer_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                }
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        order_table = next(
            table
            for table in result.sources["sources"][0]["tables"]
            if table["name"] == "Order"
        )
        customer_id = order_table["columns"][0]
        assert customer_id["tests"] == [
            "not_null",
            {"relationships": {"to": "source('bronze', 'Customer')", "field": "customer_id"}},
        ]
    finally:
        tmp.cleanup()


def test_sources_yml_skips_unresolved_and_composite_relationships() -> None:
    """FK tests are skipped when the reference is not source-local and single-column."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "OrderLine",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                {"name": "line_id", "sql_type": "INT", "is_nullable": False},
                {"name": "customer_id", "sql_type": "INT", "is_nullable": True},
            ],
            "foreign_keys": [
                {
                    "constraint_name": "FK_OrderLine_Order",
                    "columns": ["order_id", "line_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Order",
                    "referenced_columns": ["order_id", "line_id"],
                },
                {
                    "constraint_name": "FK_OrderLine_Customer",
                    "columns": ["customer_id"],
                    "referenced_schema": "silver",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                },
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null"]
        assert columns[1]["tests"] == ["not_null"]
        assert "tests" not in columns[2]
    finally:
        tmp.cleanup()
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_adds_relationship_for_confirmed_source_reference \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_skips_unresolved_and_composite_relationships \
  -q
```

Expected: first test FAIL because relationship tests are missing.

- [ ] **Step 3: Implement source-local relationship derivation**

Change `_build_source_columns` to accept confirmed source FQNs:

```python
def _build_source_columns(cat: dict[str, Any], confirmed_sources: set[str]) -> list[dict[str, Any]]:
```

Add this helper:

```python
def _relationship_tests_by_column(cat: dict[str, Any], confirmed_sources: set[str]) -> dict[str, list[dict[str, Any]]]:
    tests_by_column: dict[str, list[dict[str, Any]]] = {}
    for fk in cat.get("foreign_keys", []):
        if not isinstance(fk, dict):
            continue
        columns = fk.get("columns")
        referenced_columns = fk.get("referenced_columns")
        referenced_schema = str(fk.get("referenced_schema", "")).lower()
        referenced_table = str(fk.get("referenced_table", ""))
        if (
            not isinstance(columns, list)
            or not isinstance(referenced_columns, list)
            or len(columns) != 1
            or len(referenced_columns) != 1
            or not referenced_schema
            or not referenced_table
        ):
            continue
        referenced_fqn = f"{referenced_schema}.{referenced_table.lower()}"
        if referenced_fqn not in confirmed_sources:
            continue
        local_column = str(columns[0]).strip()
        referenced_column = str(referenced_columns[0]).strip()
        if not local_column or not referenced_column:
            continue
        test = {
            "relationships": {
                "to": f"source('{referenced_schema}', '{referenced_table}')",
                "field": referenced_column,
            }
        }
        tests_by_column.setdefault(local_column.lower(), []).append(test)
    return tests_by_column
```

In `_build_source_columns`, compute relationship tests and append them after uniqueness:

```python
    relationships_by_column = _relationship_tests_by_column(cat, confirmed_sources)
```

```python
        for relationship_test in relationships_by_column.get(str(name).lower(), []):
            _append_test(tests, relationship_test)
```

Build `confirmed_sources` after catalog iteration:

```python
    confirmed_sources = set(included)
```

Pass it when building columns:

```python
            columns = _build_source_columns(cat, confirmed_sources)
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add lib/shared/generate_sources.py tests/unit/generate_sources/test_generate_sources.py
git commit -m "VU-1099: derive safe source relationships"
```

## Task 4: Explicit Source Freshness

**Files:**

- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Modify: `lib/shared/generate_sources.py`

- [ ] **Step 1: Add failing tests for explicit watermark freshness**

Append these tests:

```python
def test_sources_yml_uses_profile_watermark_for_freshness_when_column_exists() -> None:
    """profile.watermark.column becomes loaded_at_field only when it is an emitted source column."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "profile": {"watermark": {"column": "loaded_at", "source": "llm"}},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert table["loaded_at_field"] == "loaded_at"
        assert table["freshness"] == {
            "warn_after": {"count": 24, "period": "hour"},
            "error_after": {"count": 48, "period": "hour"},
        }
    finally:
        tmp.cleanup()


def test_sources_yml_skips_freshness_without_usable_profile_watermark() -> None:
    """Change-capture flags and missing watermark columns do not emit source freshness."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "profile": {"watermark": {"column": "missing_loaded_at", "source": "llm"}},
            "change_capture": {"enabled": True, "mechanism": "change_tracking"},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert "loaded_at_field" not in table
        assert "freshness" not in table
    finally:
        tmp.cleanup()
```

- [ ] **Step 2: Run the new tests and verify they fail**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_uses_profile_watermark_for_freshness_when_column_exists \
  ../tests/unit/generate_sources/test_generate_sources.py::test_sources_yml_skips_freshness_without_usable_profile_watermark \
  -q
```

Expected: first test FAIL because `loaded_at_field` and `freshness` are missing.

- [ ] **Step 3: Implement profile-watermark freshness**

Add constants near the Typer app:

```python
DEFAULT_SOURCE_FRESHNESS = {
    "warn_after": {"count": 24, "period": "hour"},
    "error_after": {"count": 48, "period": "hour"},
}
```

Add this helper:

```python
def _source_loaded_at_field(cat: dict[str, Any], columns: list[dict[str, Any]]) -> str | None:
    profile = cat.get("profile")
    if not isinstance(profile, dict):
        return None
    watermark = profile.get("watermark")
    if not isinstance(watermark, dict):
        return None
    column = str(watermark.get("column", "")).strip()
    if not column:
        return None
    emitted_columns = {str(entry["name"]).lower(): str(entry["name"]) for entry in columns}
    return emitted_columns.get(column.lower())
```

When building each table entry, after columns are built and attached:

```python
            loaded_at_field = _source_loaded_at_field(cat, columns)
            if loaded_at_field:
                table_entry["loaded_at_field"] = loaded_at_field
                table_entry["freshness"] = DEFAULT_SOURCE_FRESHNESS
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit Task 4**

```bash
git add lib/shared/generate_sources.py tests/unit/generate_sources/test_generate_sources.py
git commit -m "VU-1099: emit explicit source freshness"
```

## Task 5: Write Path and Idempotency Verification

**Files:**

- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Optionally modify: `lib/shared/generate_sources.py`

- [ ] **Step 1: Add coverage for written enriched YAML and idempotent rewrites**

Append this test:

```python
def test_write_sources_yml_writes_enriched_yaml_idempotently(tmp_path: Path) -> None:
    """write_sources_yml writes enriched YAML and stable repeated output."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "bronze.customer.json").write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "primary_keys": [{"constraint_name": "PK_Customer", "columns": ["customer_id"]}],
            "profile": {"watermark": {"column": "loaded_at"}},
        }),
        encoding="utf-8",
    )
    dbt_dir = tmp_path / "dbt"
    (dbt_dir / "models" / "staging").mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "i"],
        cwd=tmp_path, capture_output=True, check=True,
        env={
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@t",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@t",
            "HOME": str(Path.home()),
        },
    )

    first = write_sources_yml(tmp_path)
    assert first.path is not None
    sources_path = Path(first.path)
    first_content = sources_path.read_text(encoding="utf-8")

    second = write_sources_yml(tmp_path)
    assert second.path == first.path
    assert sources_path.read_text(encoding="utf-8") == first_content
    assert "data_type: INT" in first_content
    assert "- not_null" in first_content
    assert "- unique" in first_content
    assert "loaded_at_field: loaded_at" in first_content
```

- [ ] **Step 2: Run the new test**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources/test_generate_sources.py::test_write_sources_yml_writes_enriched_yaml_idempotently -q
```

Expected: PASS if prior tasks preserved deterministic output. If it fails due to YAML ordering or aliases, adjust `generate_sources.py` so output uses fresh plain dicts and deterministic sorted tables.

- [ ] **Step 3: Run setup-target regression tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources ../tests/unit/target_setup -q
```

Expected: PASS.

- [ ] **Step 4: Commit Task 5**

```bash
git add lib/shared/generate_sources.py tests/unit/generate_sources/test_generate_sources.py
git commit -m "VU-1099: verify enriched source yaml writes"
```

## Final Verification

- [ ] **Step 1: Run changed-area unit tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/generate_sources ../tests/unit/target_setup
```

Expected: PASS.

- [ ] **Step 2: Run broader shared Python unit tests**

Run:

```bash
cd lib && uv run pytest
```

Expected: PASS.

- [ ] **Step 3: Run markdown lint for new docs**

Run:

```bash
markdownlint docs/design/README.md docs/design/source-yaml-catalog-enrichment/README.md docs/superpowers/plans/2026-04-16-catalog-derived-source-yaml.md
```

Expected: PASS.

- [ ] **Step 4: Manual tests**

No manual tests required.
