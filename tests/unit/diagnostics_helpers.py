"""Shared test helpers for diagnostic check tests.

Used by test_diagnostics.py, test_diagnostics_sqlserver.py, and test_diagnostics_oracle.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.catalog import write_json
from shared.diagnostics import CatalogContext
from shared.loader_data import DdlEntry


def diag_git_init(path: Path) -> None:
    """Create a minimal .git directory so load_directory recognises the project root."""
    (path / ".git").mkdir(exist_ok=True)


def diag_empty_refs() -> dict[str, Any]:
    """Return an empty references structure covering all catalog buckets."""
    return {
        "tables": {"in_scope": [], "out_of_scope": []},
        "views": {"in_scope": [], "out_of_scope": []},
        "functions": {"in_scope": [], "out_of_scope": []},
        "procedures": {"in_scope": [], "out_of_scope": []},
    }


def diag_write_catalog(root: Path, bucket: str, fqn: str, data: dict[str, Any]) -> None:
    """Write a catalog JSON file into the appropriate bucket directory."""
    d = root / "catalog" / bucket
    d.mkdir(parents=True, exist_ok=True)
    write_json(d / f"{fqn}.json", data)


def diag_write_ddl(root: Path, fqn: str, ddl: str) -> None:
    """Write a DDL file into the flat ddl/ directory (load_directory reads from here)."""
    d = root / "ddl"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{fqn}.sql").write_text(ddl, encoding="utf-8")


def diag_make_ctx(
    root: Path,
    fqn: str,
    object_type: str,
    catalog_data: dict[str, Any],
    **kwargs: Any,
) -> CatalogContext:
    """Create a CatalogContext for diagnostic check tests."""
    return CatalogContext(
        project_root=root,
        dialect=kwargs.get("dialect", "tsql"),
        fqn=fqn,
        object_type=object_type,
        catalog_data=catalog_data,
        known_fqns=kwargs.get(
            "known_fqns",
            {
                "tables": set(),
                "views": set(),
                "functions": set(),
                "procedures": set(),
            },
        ),
        ddl_entry=kwargs.get("ddl_entry"),
        pass1_results=kwargs.get("pass1_results"),
        package_members=kwargs.get("package_members"),
    )
