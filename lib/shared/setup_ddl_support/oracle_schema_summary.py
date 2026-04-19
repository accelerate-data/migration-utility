"""Oracle source schema summary helpers for setup-ddl discovery."""

from __future__ import annotations

from typing import Any


def build_oracle_schema_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entry_type = dict[str, Any]
    buckets: dict[str, entry_type] = {}
    for row in rows:
        owner = row.get("OWNER") or row.get("owner") or ""
        obj_type = (row.get("OBJECT_TYPE") or row.get("object_type") or "").upper()
        if not owner:
            continue
        if owner not in buckets:
            buckets[owner] = {
                "owner": owner,
                "tables": 0,
                "procedures": 0,
                "views": 0,
                "functions": 0,
                "materialized_views": 0,
            }
        if obj_type == "TABLE":
            buckets[owner]["tables"] += 1
        elif obj_type == "PROCEDURE":
            buckets[owner]["procedures"] += 1
        elif obj_type == "VIEW":
            buckets[owner]["views"] += 1
        elif obj_type == "MATERIALIZED VIEW":
            buckets[owner]["materialized_views"] += 1
        elif obj_type == "FUNCTION":
            buckets[owner]["functions"] += 1
    return sorted(buckets.values(), key=lambda x: x["owner"])
