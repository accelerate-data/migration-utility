from __future__ import annotations

import json

from shared.dry_run_support.readiness_context import (
    ObjectReadinessContext,
    catalog_error_output,
    load_object_context,
    object_applicability,
)
from shared.dry_run_support.common import detail
from tests.unit.dry_run.dry_run_test_helpers import _make_project


def test_load_object_context_returns_normalized_table_context() -> None:
    tmp, root = _make_project()
    with tmp:
        ctx = load_object_context(root, "silver.DimCustomer")

        assert isinstance(ctx, ObjectReadinessContext)
        assert ctx.fqn == "silver.dimcustomer"
        assert ctx.obj_type == "table"
        assert ctx.catalog is not None


def test_object_applicability_reports_source_table_not_applicable() -> None:
    tmp, root = _make_project()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        table = json.loads(table_path.read_text(encoding="utf-8"))
        table["is_source"] = True
        table_path.write_text(json.dumps(table), encoding="utf-8")
        ctx = load_object_context(root, "silver.DimCustomer")
        assert isinstance(ctx, ObjectReadinessContext)

        result = object_applicability(stage="scope", project=detail(True, "ok"), ctx=ctx)

        assert result is not None
        assert result.object is not None
        assert result.object.code == "SOURCE_TABLE"


def test_catalog_error_output_blocks_error_severity() -> None:
    tmp, root = _make_project()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        table = json.loads(table_path.read_text(encoding="utf-8"))
        table["errors"] = [{"code": "DDL_PARSE_ERROR", "severity": "error"}]
        table_path.write_text(json.dumps(table), encoding="utf-8")
        ctx = load_object_context(root, "silver.DimCustomer")
        assert isinstance(ctx, ObjectReadinessContext)

        result = catalog_error_output(stage="test-gen", project=detail(True, "ok"), ctx=ctx)

        assert result is not None
        assert result.object is not None
        assert result.object.reason == "catalog_errors_unresolved"
