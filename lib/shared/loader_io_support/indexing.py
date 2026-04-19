"""Catalog indexing helpers for shared.loader_io."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from shared.loader_data import CatalogLoadError, DdlCatalog, DdlEntry, DdlParseError
from shared.loader_io_support.manifest import read_manifest
from shared.loader_parse import extract_refs

_CATALOG_SCHEMA_VERSION = "1.0"


def _write_per_object_files(
    catalog: DdlCatalog, out: Path,
) -> dict[str, dict]:
    """Write per-object .sql files and extract refs for catalog.json."""
    bucket_map = {
        "tables": catalog.tables,
        "procedures": catalog.procedures,
        "views": catalog.views,
        "functions": catalog.functions,
    }

    objects: dict[str, dict] = {}
    for bucket, entries in bucket_map.items():
        if not entries:
            continue
        bucket_dir = out / bucket
        bucket_dir.mkdir(exist_ok=True)
        for name, entry in entries.items():
            file_name = f"{name}.sql"
            (bucket_dir / file_name).write_text(entry.raw_ddl, encoding="utf-8")

            type_label = bucket.rstrip("s")
            refs_dict: dict = {"writes_to": [], "reads_from": [], "calls": []}
            parse_error: str | None = entry.parse_error
            if parse_error is None:
                try:
                    refs = extract_refs(entry)
                    refs_dict = {
                        "writes_to": refs.writes_to,
                        "reads_from": refs.reads_from,
                        "calls": refs.calls,
                    }
                except DdlParseError as exc:
                    parse_error = str(exc)

            objects[name] = {
                "type": type_label,
                "file": f"{bucket}/{file_name}",
                **refs_dict,
                "parse_error": parse_error,
            }
    return objects


def index_directory(
    project_root: Path | str,
    output_dir: Path | str,
    dialect: str = "tsql",
) -> None:
    """Split a DDL directory into per-object files and write a catalog.json."""
    from shared.loader_io_support.directory import load_directory

    _manifest = read_manifest(Path(project_root))
    dialect = _manifest["dialect"]
    catalog = load_directory(project_root, dialect=dialect)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    objects = _write_per_object_files(catalog, out)

    catalog_doc = {
        "schema_version": _CATALOG_SCHEMA_VERSION,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "source": str(Path(project_root).resolve()),
        "objects": objects,
    }
    (out / "catalog.json").write_text(
        json.dumps(catalog_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_catalog(output_dir: Path | str) -> DdlCatalog:
    """Load a DdlCatalog from a pre-built index directory."""
    out = Path(output_dir)
    catalog_path = out / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found in {out}")

    try:
        doc = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(catalog_path), exc) from exc
    objects = doc.get("objects", {})

    result = DdlCatalog()
    bucket_attr = {
        "table": "tables",
        "procedure": "procedures",
        "view": "views",
        "function": "functions",
    }

    for name, obj in objects.items():
        file_path = out / obj["file"]
        raw_ddl = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
        entry = DdlEntry(
            raw_ddl=raw_ddl,
            ast=None,
            parse_error=obj.get("parse_error"),
        )
        attr = bucket_attr.get(obj.get("type", ""), "")
        if attr:
            getattr(result, attr)[name] = entry

    return result

