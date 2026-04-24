# Oracle Extract Boundary Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split Oracle metadata extraction into query specs, DDL fallback helpers, extraction services, and a small public orchestration facade without changing staging output behavior.

**Architecture:** Keep `shared.oracle_extract` as the compatibility facade that exports the existing private helper names and public `run_oracle_extraction`. Move SQL string construction to `oracle_extract_queries.py`, DBMS_METADATA and LONG fallback behavior to `oracle_extract_ddl.py`, and cursor-to-row extraction services to `oracle_extract_services.py`. Preserve the existing staging filenames, row shapes, logging events, connection lifecycle, and error behavior.

**Tech Stack:** Python 3.11, pytest, oracledb mocks, existing `shared.db_connect.cursor_to_dicts`, and setup-ddl staging helpers.

---

## File Structure

- Create `lib/shared/oracle_extract_queries.py`
  - Owns Oracle metadata SQL builders and query constants.
  - Exposes one function per query: `definitions_object_sql`, `view_text_sql`, `table_columns_sql`, `pk_unique_sql`, `foreign_keys_sql`, `identity_columns_sql`, `object_types_sql`, `invalid_object_types_sql`, `dmf_sql`, `proc_params_sql`, and `packages_sql`.
- Create `lib/shared/oracle_extract_ddl.py`
  - Owns CLOB coercion, procedure/function DBMS_METADATA reads, view DDL reconstruction, LONG truncation fallback, and Oracle object-type class mapping.
  - Exposes `oracle_type_to_class_desc`, `read_metadata_ddl`, `definition_from_view_text`, `extract_definition_rows`, and `extract_view_ddl_rows`.
- Create `lib/shared/oracle_extract_services.py`
  - Owns table/constraint/dependency/object/argument/package extraction functions and staging JSON writes.
  - Exposes `write_oracle_staging_json`, `extract_table_columns`, `extract_pk_unique`, `extract_foreign_keys`, `extract_identity_columns`, `extract_object_types`, `extract_dmf`, `extract_proc_params`, and `extract_packages`.
- Modify `lib/shared/oracle_extract.py`
  - Keep a small facade/orchestrator.
  - Re-export legacy helper names: `_oracle_type_to_class_desc`, `_extract_definitions`, `_extract_view_ddl`, `_extract_table_columns`, `_oracle_column_length`, `_extract_pk_unique`, `_extract_foreign_keys`, `_extract_identity_columns`, `_extract_object_types`, `_extract_dmf`, `_extract_proc_params`, `_extract_packages`.
- Add tests:
  - `tests/unit/setup_ddl/test_oracle_extract_queries.py`
  - `tests/unit/setup_ddl/test_oracle_extract_ddl.py`
  - `tests/unit/setup_ddl/test_oracle_extract_services.py`
  - `tests/unit/setup_ddl/test_oracle_extract_boundaries.py`
- Modify `tests/unit/setup_ddl/test_oracle_extract.py`
  - Keep existing compatibility imports working during the split.
- Modify `repo-map.json`
  - Update the shared Python description to mention the split Oracle extraction modules.

---

## Task 1: Oracle Query Specs

**Files:**

- Create: `lib/shared/oracle_extract_queries.py`
- Test: `tests/unit/setup_ddl/test_oracle_extract_queries.py`

- [x] **Step 1: Write failing query-spec tests**

Create `tests/unit/setup_ddl/test_oracle_extract_queries.py`:

```python
"""Tests for Oracle extraction SQL builders."""

from __future__ import annotations

import pytest


def test_view_text_sql_scopes_uppercase_owners() -> None:
    from shared.oracle_extract_queries import view_text_sql

    sql = view_text_sql(["sh", "hr"])

    assert "FROM ALL_VIEWS" in sql
    assert "OWNER IN ('SH', 'HR')" in sql
    assert "ORDER BY OWNER, VIEW_NAME" in sql


def test_object_type_sql_includes_materialized_views_and_valid_status() -> None:
    from shared.oracle_extract_queries import object_types_sql

    sql = object_types_sql(["SH"])

    assert "'MATERIALIZED VIEW'" in sql
    assert "STATUS = 'VALID'" in sql
    assert "FROM ALL_OBJECTS" in sql


def test_dmf_sql_rejects_unknown_dependency_type() -> None:
    from shared.oracle_extract_queries import dmf_sql

    with pytest.raises(ValueError, match="dep_type must be one of"):
        dmf_sql(["SH"], "PACKAGE")


def test_dmf_sql_maps_dependency_type_into_filter() -> None:
    from shared.oracle_extract_queries import dmf_sql

    sql = dmf_sql(["SH"], "PROCEDURE")

    assert "FROM ALL_DEPENDENCIES" in sql
    assert "WHERE TYPE = 'PROCEDURE'" in sql
    assert "OWNER IN ('SH')" in sql
```

- [x] **Step 2: Verify red**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_queries.py -q
```

Expected: fails with `ModuleNotFoundError: No module named 'shared.oracle_extract_queries'`.

- [x] **Step 3: Implement SQL builders**

Create `lib/shared/oracle_extract_queries.py` by moving SQL construction out of `oracle_extract.py`. Keep SQL text equivalent to the original implementation.

Required implementation outline:

```python
from __future__ import annotations

from shared.setup_ddl_support.db_helpers import build_schema_in_clause

VALID_DEP_TYPES = {"PROCEDURE", "VIEW", "FUNCTION"}


def _owners(schemas: list[str]) -> str:
    return build_schema_in_clause(schemas, uppercase=True)


def definitions_object_sql(schemas: list[str]) -> str:
    owners = _owners(schemas)
    return f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
          AND OWNER IN ({owners})
        ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
        """
```

Add the remaining query functions with the exact SQL currently embedded in `oracle_extract.py`.

- [x] **Step 4: Verify green**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_queries.py -q
```

Expected: all tests pass.

- [x] **Step 5: Commit**

```bash
git add lib/shared/oracle_extract_queries.py tests/unit/setup_ddl/test_oracle_extract_queries.py
git commit -m "refactor: add oracle extraction query specs"
```

---

## Task 2: Oracle DDL Helpers

**Files:**

- Create: `lib/shared/oracle_extract_ddl.py`
- Modify: `lib/shared/oracle_extract.py`
- Test: `tests/unit/setup_ddl/test_oracle_extract_ddl.py`

- [x] **Step 1: Write failing DDL-helper tests**

Create `tests/unit/setup_ddl/test_oracle_extract_ddl.py`:

```python
"""Tests for Oracle extraction DDL helpers."""

from __future__ import annotations

from unittest.mock import MagicMock


def test_definition_from_view_text_uses_clean_create_view_shape() -> None:
    from shared.oracle_extract_ddl import definition_from_view_text

    definition = definition_from_view_text("SH", "PROFITS", "select 1 from dual")

    assert definition == "CREATE OR REPLACE VIEW SH.PROFITS AS\nselect 1 from dual"


def test_extract_view_ddl_rows_falls_back_for_truncated_long_text() -> None:
    from shared.oracle_extract_ddl import extract_view_ddl_rows

    fallback_ddl = "CREATE OR REPLACE VIEW SH.PROFITS AS SELECT 1 FROM DUAL"
    truncated_text = "x" * 32767
    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
    main_cur.fetchall.return_value = [("SH", "PROFITS", truncated_text)]
    ddl_cur = MagicMock()
    clob = MagicMock()
    clob.read.return_value = fallback_ddl
    ddl_cur.fetchone.return_value = (clob,)
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_view_ddl_rows(conn, ["SH"])

    assert result == [{"schema_name": "SH", "object_name": "PROFITS", "definition": fallback_ddl}]


def test_extract_view_ddl_rows_keeps_truncated_diagnostic_when_metadata_fails() -> None:
    from shared.oracle_extract_ddl import extract_view_ddl_rows

    truncated_text = "x" * 32767
    main_cur = MagicMock()
    main_cur.description = [("OWNER",), ("VIEW_NAME",), ("TEXT",)]
    main_cur.fetchall.return_value = [("SH", "PROFITS", truncated_text)]
    ddl_cur = MagicMock()
    ddl_cur.execute.side_effect = RuntimeError("metadata denied")
    conn = MagicMock()
    conn.cursor.side_effect = [main_cur, ddl_cur]

    result = extract_view_ddl_rows(conn, ["SH"])

    assert result == [
        {
            "schema_name": "SH",
            "object_name": "PROFITS",
            "definition": f"CREATE OR REPLACE VIEW SH.PROFITS AS\n{truncated_text}",
            "long_truncation": True,
        }
    ]
```

- [x] **Step 2: Verify red**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_ddl.py -q
```

Expected: fails with `ModuleNotFoundError: No module named 'shared.oracle_extract_ddl'`.

- [x] **Step 3: Implement DDL helper module**

Move these behaviors into `lib/shared/oracle_extract_ddl.py`:

- `_oracle_type_to_class_desc` -> `oracle_type_to_class_desc`
- DBMS_METADATA CLOB extraction into `read_metadata_ddl`
- `_extract_definitions` -> `extract_definition_rows`
- `_extract_view_ddl` -> `extract_view_ddl_rows`

Use `definitions_object_sql` and `view_text_sql` from `oracle_extract_queries.py`.

- [x] **Step 4: Keep legacy facade imports**

In `lib/shared/oracle_extract.py`, import and alias:

```python
from shared.oracle_extract_ddl import (
    extract_definition_rows as _extract_definitions,
    extract_view_ddl_rows as _extract_view_ddl,
    oracle_type_to_class_desc as _oracle_type_to_class_desc,
)
```

Do not change `run_oracle_extraction` yet.

- [x] **Step 5: Verify green plus compatibility**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_ddl.py ../tests/unit/setup_ddl/test_oracle_extract.py -q
```

Expected: all tests pass.

- [x] **Step 6: Commit**

```bash
git add lib/shared/oracle_extract_ddl.py lib/shared/oracle_extract.py tests/unit/setup_ddl/test_oracle_extract_ddl.py
git commit -m "refactor: split oracle DDL extraction helpers"
```

---

## Task 3: Oracle Extraction Services

**Files:**

- Create: `lib/shared/oracle_extract_services.py`
- Modify: `lib/shared/oracle_extract.py`
- Test: `tests/unit/setup_ddl/test_oracle_extract_services.py`

- [x] **Step 1: Write failing service tests**

Create `tests/unit/setup_ddl/test_oracle_extract_services.py`:

```python
"""Tests for Oracle extraction service functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_extract_table_columns_uses_char_length_for_oracle_text() -> None:
    from shared.oracle_extract_services import extract_table_columns

    cur = MagicMock()
    cur.description = [
        ("OWNER",),
        ("TABLE_NAME",),
        ("COLUMN_NAME",),
        ("COLUMN_ID",),
        ("DATA_TYPE",),
        ("DATA_LENGTH",),
        ("CHAR_LENGTH",),
        ("DATA_PRECISION",),
        ("DATA_SCALE",),
        ("NULLABLE",),
        ("IDENTITY_COLUMN",),
    ]
    cur.fetchall.return_value = [
        ("SH", "CUSTOMERS", "NAME", 1, "NVARCHAR2", 80, 20, None, None, "Y", "NO"),
        ("SH", "CUSTOMERS", "TOKEN", 2, "RAW", 16, 16, None, None, "N", "NO"),
    ]
    conn = MagicMock()
    conn.cursor.return_value = cur

    result = extract_table_columns(conn, ["SH"])

    assert result[0]["max_length"] == 20
    assert result[1]["max_length"] == 16


def test_extract_object_types_returns_materialized_view_fqns() -> None:
    from shared.oracle_extract_services import extract_object_types

    valid_cur = MagicMock()
    valid_cur.description = [("OWNER",), ("OBJECT_NAME",), ("OBJECT_TYPE",)]
    valid_cur.fetchall.return_value = [
        ("SH", "SALES", "TABLE"),
        ("SH", "PROFITS", "MATERIALIZED VIEW"),
    ]
    invalid_cur = MagicMock()
    invalid_cur.description = [("OWNER",), ("OBJECT_NAME",), ("OBJECT_TYPE",), ("STATUS",)]
    invalid_cur.fetchall.return_value = []
    conn = MagicMock()
    conn.cursor.side_effect = [valid_cur, invalid_cur]

    rows, mv_fqns = extract_object_types(conn, ["SH"])

    assert rows == [
        {"schema_name": "SH", "name": "SALES", "type": "U"},
        {"schema_name": "SH", "name": "PROFITS", "type": "V"},
    ]
    assert mv_fqns == ["sh.profits"]


def test_extract_dmf_rejects_unknown_dependency_type() -> None:
    from shared.oracle_extract_services import extract_dmf

    with pytest.raises(ValueError, match="dep_type must be one of"):
        extract_dmf(MagicMock(), ["SH"], "PACKAGE")
```

- [x] **Step 2: Verify red**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_services.py -q
```

Expected: fails with `ModuleNotFoundError: No module named 'shared.oracle_extract_services'`.

- [x] **Step 3: Implement service module**

Move these functions into `lib/shared/oracle_extract_services.py`:

- `_write` -> `write_oracle_staging_json`
- `_extract_table_columns` -> `extract_table_columns`
- `_oracle_column_length` -> `oracle_column_length`
- `_extract_pk_unique` -> `extract_pk_unique`
- `_extract_foreign_keys` -> `extract_foreign_keys`
- `_extract_identity_columns` -> `extract_identity_columns`
- `_extract_object_types` -> `extract_object_types`
- `_extract_dmf` -> `extract_dmf`
- `_extract_proc_params` -> `extract_proc_params`
- `_extract_packages` -> `extract_packages`

Use SQL builders from `oracle_extract_queries.py` and `oracle_type_to_class_desc` from `oracle_extract_ddl.py`.

- [x] **Step 4: Keep legacy facade imports**

In `lib/shared/oracle_extract.py`, import and alias service functions back to the old private names:

```python
from shared.oracle_extract_services import (
    extract_dmf as _extract_dmf,
    extract_foreign_keys as _extract_foreign_keys,
    extract_identity_columns as _extract_identity_columns,
    extract_object_types as _extract_object_types,
    extract_packages as _extract_packages,
    extract_pk_unique as _extract_pk_unique,
    extract_proc_params as _extract_proc_params,
    extract_table_columns as _extract_table_columns,
    oracle_column_length as _oracle_column_length,
    write_oracle_staging_json as _write,
)
```

- [x] **Step 5: Verify green plus compatibility**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_services.py ../tests/unit/setup_ddl/test_oracle_extract.py -q
```

Expected: all tests pass.

- [x] **Step 6: Commit**

```bash
git add lib/shared/oracle_extract_services.py lib/shared/oracle_extract.py tests/unit/setup_ddl/test_oracle_extract_services.py
git commit -m "refactor: split oracle extraction services"
```

---

## Task 4: Oracle Extract Facade And Orchestration

**Files:**

- Modify: `lib/shared/oracle_extract.py`
- Test: `tests/unit/setup_ddl/test_oracle_extract_boundaries.py`

- [x] **Step 1: Write facade boundary tests**

Create `tests/unit/setup_ddl/test_oracle_extract_boundaries.py`:

```python
"""Import-boundary tests for split Oracle extraction modules."""

from __future__ import annotations


def test_oracle_extract_facade_exports_legacy_helpers() -> None:
    from shared import oracle_extract

    assert callable(oracle_extract.run_oracle_extraction)
    assert callable(oracle_extract._extract_definitions)
    assert callable(oracle_extract._extract_view_ddl)
    assert callable(oracle_extract._extract_table_columns)
    assert callable(oracle_extract._oracle_column_length)
    assert callable(oracle_extract._extract_pk_unique)
    assert callable(oracle_extract._extract_foreign_keys)
    assert callable(oracle_extract._extract_identity_columns)
    assert callable(oracle_extract._extract_object_types)
    assert callable(oracle_extract._extract_dmf)
    assert callable(oracle_extract._extract_proc_params)
    assert callable(oracle_extract._extract_packages)


def test_split_modules_export_owned_entrypoints() -> None:
    from shared import oracle_extract_ddl, oracle_extract_queries, oracle_extract_services

    assert callable(oracle_extract_queries.table_columns_sql)
    assert callable(oracle_extract_ddl.extract_view_ddl_rows)
    assert callable(oracle_extract_services.extract_table_columns)
```

- [x] **Step 2: Verify red if Task 4 starts before facade cleanup**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_boundaries.py -q
```

Expected: passes if Tasks 1-3 already added exports; otherwise fails on missing modules.

- [x] **Step 3: Reduce `oracle_extract.py` to orchestration**

Remove moved implementation bodies from `lib/shared/oracle_extract.py`. Keep only:

- imports
- `logger`
- compatibility aliases
- `run_oracle_extraction`
- `__all__`

Make `run_oracle_extraction` call the imported aliases or public service names in the same order as before.

- [x] **Step 4: Verify facade and existing Oracle tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract_boundaries.py ../tests/unit/setup_ddl/test_oracle_extract.py -q
```

Expected: all tests pass.

- [x] **Step 5: Commit**

```bash
git add lib/shared/oracle_extract.py tests/unit/setup_ddl/test_oracle_extract_boundaries.py
git commit -m "refactor: make oracle extraction facade explicit"
```

---

## Task 5: Repo Map, Full Verification, And Review

**Files:**

- Modify: `repo-map.json`

- [x] **Step 1: Update repo map**

In `repo-map.json`, replace the `oracle_extract.py` description in `modules.shared_python.description` with language equivalent to:

```text
oracle_extract.py (direct Oracle extraction orchestration facade; called by run_extract), oracle_extract_queries.py (Oracle metadata SQL builders), oracle_extract_ddl.py (DBMS_METADATA and ALL_VIEWS LONG fallback DDL helpers), oracle_extract_services.py (Oracle metadata row extraction and staging writes)
```

- [x] **Step 2: Validate JSON**

Run:

```bash
python -m json.tool repo-map.json >/tmp/repo-map-check.json
```

Expected: exit 0.

- [x] **Step 3: Run focused unit verification**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl/test_oracle_extract.py ../tests/unit/setup_ddl/test_oracle_extract_queries.py ../tests/unit/setup_ddl/test_oracle_extract_ddl.py ../tests/unit/setup_ddl/test_oracle_extract_services.py ../tests/unit/setup_ddl/test_oracle_extract_boundaries.py ../tests/unit/setup_ddl/test_extraction.py ../tests/unit/setup_ddl/test_extract_boundaries.py -q
```

Expected: all tests pass.

- [x] **Step 4: Run full setup-ddl unit suite**

Run:

```bash
cd lib && uv run pytest ../tests/unit/setup_ddl -q
```

Expected: all tests pass.

- [x] **Step 5: Lint plan markdown**

Run:

```bash
markdownlint docs/superpowers/plans/2026-04-19-oracle-extract-boundary-cleanup.md
```

Expected: exit 0.

- [x] **Step 6: Commit repo map and plan**

```bash
git add docs/superpowers/plans/2026-04-19-oracle-extract-boundary-cleanup.md repo-map.json
git commit -m "docs: plan oracle extraction boundary cleanup"
```

- [x] **Step 7: Final review**

Dispatch a read-only review subagent over `origin/main..HEAD` focused on Oracle extraction behavior drift, DBMS_METADATA fallback semantics, staging file names, public import contracts, and test gaps.

---

## Execution Notes

- Do not run live Oracle integration tests unless explicitly requested; they require local Oracle Docker/env setup.
- Keep private helper compatibility aliases in `shared.oracle_extract` because existing unit tests and potential internal consumers import them.
- Preserve the exact staging filenames written by `run_oracle_extraction`.
- Preserve connection lifecycle: `_oracle_connect()` once, close in `finally`.
- Preserve current warning events: `oracle_ddl_skip`, `oracle_view_long_truncation`, and `oracle_invalid_object`.
- Avoid combining this work with target setup seeds.
