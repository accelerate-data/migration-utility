from __future__ import annotations

from typing import Any

from shared.dmf_processing import empty_scoped


def ensure_references(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a proc/view/function catalog dict has a full references structure."""
    if "references" not in data:
        data["references"] = {
            "tables": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
            "procedures": empty_scoped(),
        }
    refs = data["references"]
    for bucket in ("tables", "views", "functions", "procedures"):
        if bucket not in refs:
            refs[bucket] = empty_scoped()
        if "in_scope" not in refs[bucket]:
            refs[bucket]["in_scope"] = []
        if "out_of_scope" not in refs[bucket]:
            refs[bucket]["out_of_scope"] = []
    return data


def ensure_referenced_by(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a table catalog dict has a full referenced_by structure."""
    if "referenced_by" not in data:
        data["referenced_by"] = {
            "procedures": empty_scoped(),
            "views": empty_scoped(),
            "functions": empty_scoped(),
        }
    ref_by = data["referenced_by"]
    for bucket in ("procedures", "views", "functions"):
        if bucket not in ref_by:
            ref_by[bucket] = empty_scoped()
        if "in_scope" not in ref_by[bucket]:
            ref_by[bucket]["in_scope"] = []
        if "out_of_scope" not in ref_by[bucket]:
            ref_by[bucket]["out_of_scope"] = []
    return data
