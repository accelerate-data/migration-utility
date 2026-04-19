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
