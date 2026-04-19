"""Catalog diagnostics compatibility barrel and CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from shared.diagnostics.context import (
    CatalogContext as CatalogContext,
    build_ddl_lookup as build_ddl_lookup,
    build_known_fqns as build_known_fqns,
    load_package_members as load_package_members,
)
from shared.diagnostics.registry import (
    ALL_DIALECTS as ALL_DIALECTS,
    DiagnosticRegistry as DiagnosticRegistry,
    DiagnosticResult as DiagnosticResult,
    _CheckSpec as _CheckSpec,
    _REGISTRY as _REGISTRY,
    diagnostic as diagnostic,
)
from shared.diagnostics.runner import (
    OBJECT_BUCKETS as OBJECT_BUCKETS,
    run_checks as run_checks,
    run_diagnostics as run_diagnostics,
    write_results as write_results,
)

_THRESHOLDS: dict[str, int] = {
    "NESTED_VIEW_CHAIN_DEPTH": 5,
    "MULTI_TABLE_READ_COUNT": 5,
}
_build_ddl_lookup = build_ddl_lookup
_build_known_fqns = build_known_fqns
_load_package_members = load_package_members
_OBJECT_BUCKETS = OBJECT_BUCKETS
_run_checks = run_checks
_write_results = write_results

__all__ = [
    "ALL_DIALECTS",
    "CatalogContext",
    "DiagnosticRegistry",
    "DiagnosticResult",
    "_CheckSpec",
    "_REGISTRY",
    "_THRESHOLDS",
    "_build_ddl_lookup",
    "_build_known_fqns",
    "_load_package_members",
    "_OBJECT_BUCKETS",
    "_run_checks",
    "_write_results",
    "diagnostic",
    "run_diagnostics",
]

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


@app.command()
def main(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Root artifacts directory"),
    dialect: Optional[str] = typer.Option(None, "--dialect", help="SQL dialect (tsql, oracle)"),
) -> None:
    """Run catalog diagnostics and write results to catalog JSON files."""
    from shared.env_config import resolve_project_root
    from shared.loader_io import read_manifest
    from shared.runtime_config import (
        get_primary_dialect,
        get_primary_technology,
        validate_supported_technologies,
    )

    root = resolve_project_root(project_root)
    if dialect is None:
        manifest = read_manifest(root)
        try:
            validate_supported_technologies(manifest)
        except ValueError as exc:
            raise typer.BadParameter(str(exc), param_hint="--dialect") from exc
        technology = get_primary_technology(manifest)
        if technology is not None:
            dialect = get_primary_dialect(manifest)
        else:
            dialect = manifest.get("dialect")
            if dialect not in {"tsql", "oracle"}:
                raise typer.BadParameter(
                    "manifest.json does not define a supported dialect. "
                    "Pass --dialect explicitly or configure sql_server/oracle runtime metadata.",
                    param_hint="--dialect",
                )

    result = run_diagnostics(root, dialect=dialect)
    typer.echo(json.dumps(result))


if __name__ == "__main__":
    app()


from shared.diagnostics import common as _common  # noqa: F401, E402
from shared.diagnostics import oracle as _oracle  # noqa: F401, E402
from shared.diagnostics import sqlserver as _sqlserver  # noqa: F401, E402
