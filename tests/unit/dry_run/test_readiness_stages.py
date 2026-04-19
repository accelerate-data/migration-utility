from __future__ import annotations

import json

from shared.dry_run_support.common import detail
from shared.dry_run_support.readiness_context import ObjectReadinessContext, load_object_context
from shared.dry_run_support.readiness_stages import generate_ready, profile_ready, project_stage_ready
from tests.unit.dry_run.dry_run_test_helpers import _make_project


def test_project_stage_ready_requires_target_for_generate() -> None:
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = project_stage_ready(root, "generate")

        assert result.ready is False
        assert result.reason == "target_not_configured"


def test_profile_ready_requires_resolved_table_scoping() -> None:
    tmp, root = _make_project()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        table = json.loads(table_path.read_text(encoding="utf-8"))
        table["scoping"]["status"] = "pending"
        table_path.write_text(json.dumps(table), encoding="utf-8")
        ctx = load_object_context(root, "silver.DimCustomer")
        assert isinstance(ctx, ObjectReadinessContext)

        result = profile_ready(root, "profile", detail(True, "ok"), ctx)

        assert result.object is not None
        assert result.object.reason == "scoping_not_resolved"


def test_generate_ready_requires_writer_refactor() -> None:
    tmp, root = _make_project()
    with tmp:
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc = json.loads(proc_path.read_text(encoding="utf-8"))
        del proc["refactor"]
        proc_path.write_text(json.dumps(proc), encoding="utf-8")
        ctx = load_object_context(root, "silver.DimCustomer")
        assert isinstance(ctx, ObjectReadinessContext)

        result = generate_ready(root, "generate", detail(True, "ok"), ctx)

        assert result.object is not None
        assert result.object.reason == "refactor_not_complete"
