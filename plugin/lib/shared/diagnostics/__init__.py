"""Catalog diagnostics --- extensible registry and two-pass runner.

Dialect-specific check modules (e.g. ``sqlserver.py``, ``oracle.py``) register
checks at import time via the ``@diagnostic`` decorator.  The ``run_diagnostics``
entry point executes registered checks in two passes and writes results into
catalog JSON files.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

import typer

from shared.catalog import has_catalog, write_json
from shared.env_config import resolve_catalog_dir
from shared.loader import DdlCatalog, DdlEntry, load_directory

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def _load_package_members(project_root: Path) -> set[str] | None:
    """Load Oracle package members from staging/packages.json.

    Returns a set of lowercased ``schema.member_name`` FQNs, or None if the
    file does not exist.
    """
    for subdir in ("staging", ".staging"):
        packages_path = project_root / subdir / "packages.json"
        if packages_path.exists():
            try:
                rows = json.loads(packages_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=load_package_members error=%s", exc)
                return None
            members: set[str] = set()
            for row in rows:
                schema = row.get("schema_name", "")
                member = row.get("member_name", "")
                if schema and member:
                    members.add(f"{schema}.{member}".lower())
            return members if members else None
    return None

# -- Thresholds ---------------------------------------------------------------

_THRESHOLDS: dict[str, int] = {
    "NESTED_VIEW_CHAIN_DEPTH": 5,
    "MULTI_TABLE_READ_COUNT": 5,
}

# -- Types --------------------------------------------------------------------

ALL_DIALECTS = ("tsql", "oracle")


@dataclass
class DiagnosticResult:
    """One diagnostic finding, written to warnings[] or errors[] in catalog JSON."""

    code: str
    message: str
    severity: str  # "error" | "warning"
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"code": self.code, "message": self.message, "severity": self.severity}
        if self.details:
            d["details"] = self.details
        return d


@dataclass
class CatalogContext:
    """Read-only context bag passed to every diagnostic check function."""

    project_root: Path
    dialect: str
    fqn: str
    object_type: str  # "view", "function", "procedure"
    catalog_data: dict[str, Any]
    known_fqns: dict[str, set[str]]  # bucket -> set of normalized FQNs
    ddl_entry: DdlEntry | None = None
    pass1_results: dict[str, list[DiagnosticResult]] | None = None
    package_members: set[str] | None = None  # Oracle package member FQNs


# Type alias for check functions
CheckFn = Callable[[CatalogContext], DiagnosticResult | list[DiagnosticResult] | None]


@dataclass
class _CheckSpec:
    fn: CheckFn
    code: str
    objects: list[str]
    dialects: tuple[str, ...]
    severity: str
    pass_number: int


# -- Registry -----------------------------------------------------------------


class DiagnosticRegistry:
    """Global registry of diagnostic check functions."""

    def __init__(self) -> None:
        self._checks: list[_CheckSpec] = []

    def register(
        self,
        fn: CheckFn,
        code: str,
        objects: list[str],
        dialects: tuple[str, ...],
        severity: str,
        pass_number: int,
    ) -> None:
        self._checks.append(_CheckSpec(fn=fn, code=code, objects=objects, dialects=dialects, severity=severity, pass_number=pass_number))

    def checks_for(self, object_type: str, dialect: str, pass_number: int) -> list[_CheckSpec]:
        return [
            c for c in self._checks
            if object_type in c.objects
            and dialect in c.dialects
            and c.pass_number == pass_number
        ]


_REGISTRY = DiagnosticRegistry()


def diagnostic(
    code: str,
    objects: list[str],
    dialects: tuple[str, ...] = ALL_DIALECTS,
    severity: str = "warning",
    pass_number: int = 1,
) -> Callable[[CheckFn], CheckFn]:
    """Decorator to register a diagnostic check function."""

    def decorator(fn: CheckFn) -> CheckFn:
        _REGISTRY.register(fn, code, objects, dialects, severity, pass_number)
        return fn

    return decorator


# -- Runner -------------------------------------------------------------------

_OBJECT_BUCKETS = ("procedures", "views", "functions")


def _build_known_fqns(catalog_dir: Path) -> dict[str, set[str]]:
    """Glob catalog directories to build a set of known FQNs per bucket."""
    known: dict[str, set[str]] = {}
    for bucket in ("tables", "procedures", "views", "functions"):
        bucket_dir = catalog_dir / bucket
        if bucket_dir.is_dir():
            known[bucket] = {p.stem for p in bucket_dir.glob("*.json")}
        else:
            known[bucket] = set()
    return known


def _build_ddl_lookup(ddl_catalog: DdlCatalog) -> dict[str, DdlEntry]:
    """Build a flat FQN -> DdlEntry lookup from a DdlCatalog."""
    lookup: dict[str, DdlEntry] = {}
    for bucket_name in ("tables", "procedures", "views", "functions"):
        for fqn, entry in getattr(ddl_catalog, bucket_name).items():
            lookup[fqn] = entry
    return lookup


def _run_checks(
    catalog_dir: Path,
    project_root: Path,
    dialect: str,
    known_fqns: dict[str, set[str]],
    ddl_lookup: dict[str, DdlEntry],
    pass_number: int,
    pass1_results: dict[str, list[DiagnosticResult]] | None = None,
    package_members: set[str] | None = None,
) -> tuple[dict[str, list[DiagnosticResult]], int]:
    """Run all checks for a given pass number across all catalog objects."""
    results: dict[str, list[DiagnosticResult]] = defaultdict(list)
    suppressed_checks = 0

    for bucket in _OBJECT_BUCKETS:
        object_type = bucket.rstrip("s")  # "procedures" -> "procedure"
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_path in sorted(bucket_dir.glob("*.json")):
            fqn = json_path.stem
            try:
                catalog_data = json.loads(json_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=diagnostics_skip fqn=%s error=%s", fqn, exc)
                continue

            ctx = CatalogContext(
                project_root=project_root,
                dialect=dialect,
                fqn=fqn,
                object_type=object_type,
                catalog_data=catalog_data,
                known_fqns=known_fqns,
                ddl_entry=ddl_lookup.get(fqn),
                pass1_results=pass1_results,
                package_members=package_members,
            )

            checks = _REGISTRY.checks_for(object_type, dialect, pass_number)
            for spec in checks:
                try:
                    result = spec.fn(ctx)
                except Exception:
                    logger.exception("event=diagnostic_check_error code=%s fqn=%s", spec.code, fqn)
                    suppressed_checks += 1
                    continue
                if result is None:
                    continue
                if isinstance(result, list):
                    results[fqn].extend(result)
                else:
                    results[fqn].append(result)

    return dict(results), suppressed_checks


def _write_results(catalog_dir: Path, all_results: dict[str, list[DiagnosticResult]]) -> tuple[int, int]:
    """Write diagnostic results into catalog JSON files. Returns (warnings_added, errors_added)."""
    warnings_added = 0
    errors_added = 0

    for bucket in _OBJECT_BUCKETS:
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_path in sorted(bucket_dir.glob("*.json")):
            fqn = json_path.stem
            diags = all_results.get(fqn, [])

            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue

            warnings = [d.to_dict() for d in diags if d.severity == "warning"]
            errors = [d.to_dict() for d in diags if d.severity == "error"]

            # Full replace for idempotency
            data["warnings"] = warnings
            data["errors"] = errors

            write_json(json_path, data)

            warnings_added += len(warnings)
            errors_added += len(errors)

    return warnings_added, errors_added


def run_diagnostics(project_root: Path, dialect: str = "tsql") -> dict[str, Any]:
    """Run all registered diagnostic checks on catalog files.

    Args:
        project_root: Root artifacts directory containing ``ddl/`` and ``catalog/``.
        dialect: SQL dialect (e.g. "tsql", "oracle").

    Returns:
        Summary dict: {"objects_checked": N, "warnings_added": N, "errors_added": N}
    """
    project_root = Path(project_root)

    if not has_catalog(project_root):
        logger.warning("event=run_diagnostics status=skip reason=no_catalog path=%s", project_root)
        return {"objects_checked": 0, "warnings_added": 0, "errors_added": 0}

    catalog_dir = resolve_catalog_dir(project_root)

    # Load DDL entries for parse_error / unsupported_syntax_nodes access
    ddl_catalog = load_directory(project_root, dialect=dialect)
    ddl_lookup = _build_ddl_lookup(ddl_catalog)

    # Build known FQN sets for MISSING_REFERENCE checks
    known_fqns = _build_known_fqns(catalog_dir)

    # Load Oracle package members when applicable
    package_members = _load_package_members(project_root) if dialect == "oracle" else None

    # Pass 1: per-object + reference resolution
    pass1_results, pass1_suppressed = _run_checks(
        catalog_dir,
        project_root,
        dialect,
        known_fqns,
        ddl_lookup,
        pass_number=1,
        package_members=package_members,
    )

    # Pass 2: graph traversal (receives pass 1 results)
    pass2_results, pass2_suppressed = _run_checks(
        catalog_dir,
        project_root,
        dialect,
        known_fqns,
        ddl_lookup,
        pass_number=2,
        pass1_results=pass1_results,
        package_members=package_members,
    )

    # Merge results
    all_results: dict[str, list[DiagnosticResult]] = defaultdict(list)
    for fqn, diags in pass1_results.items():
        all_results[fqn].extend(diags)
    for fqn, diags in pass2_results.items():
        all_results[fqn].extend(diags)

    # Write to catalog JSON files
    objects_checked = sum(
        1
        for bucket in _OBJECT_BUCKETS
        if (catalog_dir / bucket).is_dir()
        for _p in (catalog_dir / bucket).glob("*.json")
    )
    warnings_added, errors_added = _write_results(catalog_dir, dict(all_results))

    summary = {
        "objects_checked": objects_checked,
        "warnings_added": warnings_added,
        "errors_added": errors_added,
        "suppressed_checks": pass1_suppressed + pass2_suppressed,
    }
    logger.info("event=run_diagnostics_complete %s", summary)
    return summary


# -- CLI ----------------------------------------------------------------------


@app.command()
def main(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Root artifacts directory"),
    dialect: Optional[str] = typer.Option(None, "--dialect", help="SQL dialect (tsql, oracle)"),
) -> None:
    """Run catalog diagnostics and write results to catalog JSON files."""
    from shared.env_config import resolve_project_root
    from shared.loader_io import read_manifest

    root = resolve_project_root(project_root)
    if dialect is None:
        manifest = read_manifest(root)
        dialect = {"SQL Server": "tsql", "Oracle": "oracle"}.get(manifest.get("technology", ""), "tsql")

    result = run_diagnostics(root, dialect=dialect)
    typer.echo(json.dumps(result))


if __name__ == "__main__":
    app()


# Import check modules so decorators fire at import time.
from shared.diagnostics import common as _common  # noqa: F401, E402
from shared.diagnostics import sqlserver as _sqlserver  # noqa: F401, E402
from shared.diagnostics import oracle as _oracle  # noqa: F401, E402
