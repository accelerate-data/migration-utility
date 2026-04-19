"""DDL assembly helpers for setup-ddl extraction flows."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.name_resolver import normalize
from shared.setup_ddl_support.manifest import require_technology
from shared.setup_ddl_support.staging_io import read_json, read_json_optional
from shared.sql_types import format_sql_type


def run_assemble_modules(input_path: Path, project_root: Path, object_type: str) -> dict[str, Any]:
    if object_type not in ("procedures", "views", "functions"):
        raise ValueError(f"Invalid type: {object_type}. Must be procedures, views, or functions.")
    rows = read_json(input_path)
    blocks = [row.get("definition", "").strip() for row in rows if row.get("definition")]
    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / f"{object_type}.sql"
    out_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return {"file": str(out_path), "count": len(blocks)}


def run_assemble_tables(input_path: Path, project_root: Path) -> dict[str, Any]:
    rows = read_json(input_path)
    technology = require_technology(project_root)
    oracle_style = technology == "oracle"
    tables: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        tables.setdefault((row["schema_name"], row["table_name"]), []).append(row)
    for cols in tables.values():
        cols.sort(key=lambda r: r.get("column_id", 0))
    blocks: list[str] = []
    for (schema_name, table_name), cols in tables.items():
        col_defs: list[str] = []
        for col in cols:
            type_str = format_sql_type(col["type_name"], col["max_length"], col["precision"], col["scale"])
            nullable = " NOT NULL" if not col.get("is_nullable") else " NULL"
            if oracle_style:
                col_defs.append(f"    {col['column_name']} {type_str}{nullable}")
            else:
                identity = ""
                if col.get("is_identity"):
                    identity = f" IDENTITY({col.get('seed_value', 1)},{col.get('increment_value', 1)})"
                col_defs.append(f"    [{col['column_name']}] {type_str}{identity}{nullable}")
        if oracle_style:
            ddl = f"CREATE TABLE {schema_name}.{table_name} (\n" + ",\n".join(col_defs) + "\n)"
        else:
            ddl = f"CREATE TABLE [{schema_name}].[{table_name}] (\n" + ",\n".join(col_defs) + "\n)"
        blocks.append(ddl)
    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / "tables.sql"
    out_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return {"file": str(out_path), "count": len(blocks)}


def _repo_relative(project_root: Path, path: str | Path) -> str:
    raw_path = Path(path)
    resolved = raw_path if raw_path.is_absolute() else project_root / raw_path
    try:
        return str(resolved.relative_to(project_root))
    except ValueError:
        return str(raw_path)


def assemble_ddl_from_staging(staging_dir: Path, project_root: Path) -> list[str]:
    written_paths: list[str] = []
    obj_type_rows = read_json_optional(staging_dir / "object_types.json")
    type_lookup = {
        normalize(f"{row['schema_name']}.{row['name']}"): row.get("type", "").strip()
        for row in obj_type_rows
    }
    definitions_rows = read_json_optional(staging_dir / "definitions.json")
    for obj_label, type_codes in [("procedures", {"P"}), ("views", {"V"}), ("functions", {"FN", "IF", "TF"})]:
        typed_defs = [
            row
            for row in definitions_rows
            if type_lookup.get(normalize(f"{row['schema_name']}.{row['object_name']}")) in type_codes
        ]
        if typed_defs:
            typed_path = staging_dir / f"{obj_label}.json"
            typed_path.write_text(json.dumps(typed_defs, ensure_ascii=False), encoding="utf-8")
            result = run_assemble_modules(typed_path, project_root, obj_label)
            written_paths.append(_repo_relative(project_root, result["file"]))
    table_cols_path = staging_dir / "table_columns.json"
    if table_cols_path.exists():
        result = run_assemble_tables(table_cols_path, project_root)
        written_paths.append(_repo_relative(project_root, result["file"]))
    return written_paths
