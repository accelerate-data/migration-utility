"""Plan and execute target-side source table replication."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from shared.dbops import get_dbops
from shared.name_resolver import normalize
from shared.output_models.replicate_source_tables import (
    ReplicateSourceTablesOutput,
    ReplicateTableResult,
)
from shared.runtime_config import get_runtime_role
from shared.runtime_config_models import RuntimeRole
from shared.setup_ddl_support.manifest import read_manifest_strict
from shared.target_setup import get_target_source_schema

logger = logging.getLogger(__name__)

MAX_REPLICATE_LIMIT = 10000


class ReplicationAdapter(Protocol):
    def fetch_source_rows(
        self,
        schema_name: str,
        table_name: str,
        *,
        limit: int,
        predicate: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        """Fetch capped source rows."""

    def truncate_table(self, schema_name: str, table_name: str) -> None:
        """Remove all rows from one target table."""

    def insert_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: list[str],
        rows: list[tuple[object, ...]],
    ) -> int:
        """Insert rows and return the inserted row count."""


@dataclass(frozen=True)
class SourceTablePlan:
    fqn: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    columns: list[str]
    predicate: str | None = None


def _validate_limit(limit: int | None) -> int:
    if limit is None:
        raise ValueError("LIMIT_REQUIRED: --limit is required and must be between 1 and 10000.")
    if limit < 1:
        raise ValueError("LIMIT_REQUIRED: --limit is required and must be between 1 and 10000.")
    if limit > MAX_REPLICATE_LIMIT:
        raise ValueError(f"LIMIT_TOO_HIGH: --limit must be <= {MAX_REPLICATE_LIMIT}.")
    return limit


def _require_runtime_role(project_root: Path, role_name: str) -> RuntimeRole:
    manifest = read_manifest_strict(project_root)
    role = get_runtime_role(manifest, role_name)
    if role is None:
        raise ValueError(f"manifest.json is missing runtime.{role_name}. Run setup-{role_name} first.")
    return role


def _load_confirmed_source_tables(project_root: Path) -> list[SourceTablePlan]:
    target_schema = get_target_source_schema(project_root)
    tables_dir = project_root / "catalog" / "tables"
    if not tables_dir.is_dir():
        return []

    plans: list[SourceTablePlan] = []
    for table_file in sorted(tables_dir.glob("*.json")):
        payload = json.loads(table_file.read_text(encoding="utf-8"))
        if payload.get("excluded") or payload.get("is_source") is not True:
            continue
        source_schema = str(payload.get("schema", "")).strip()
        source_table = str(payload.get("name", "")).strip()
        if not source_schema or not source_table:
            continue
        columns = [
            str(column["name"])
            for column in payload.get("columns", [])
            if isinstance(column, dict) and column.get("name")
        ]
        plans.append(
            SourceTablePlan(
                fqn=normalize(f"{source_schema}.{source_table}"),
                source_schema=source_schema,
                source_table=source_table,
                target_schema=target_schema,
                target_table=source_table,
                columns=columns,
            )
        )
    return plans


def _parse_filters(filters: list[str] | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_filter in filters or []:
        if "=" not in raw_filter:
            raise ValueError("FILTER_INVALID: --filter must be formatted as <fqn>=<predicate>.")
        raw_fqn, predicate = raw_filter.split("=", 1)
        fqn = normalize(raw_fqn.strip())
        text = predicate.strip()
        if not fqn or not text:
            raise ValueError("FILTER_INVALID: --filter must be formatted as <fqn>=<predicate>.")
        parsed[fqn] = text
    return parsed


def _select_tables(
    plans: list[SourceTablePlan],
    *,
    select: list[str] | None,
    exclude: list[str] | None,
    filters: list[str] | None,
) -> list[SourceTablePlan]:
    by_fqn = {plan.fqn: plan for plan in plans}
    selected_fqns = [normalize(fqn) for fqn in select] if select else [plan.fqn for plan in plans]
    missing_selected = [fqn for fqn in selected_fqns if fqn not in by_fqn]
    if missing_selected:
        raise ValueError(f"TABLE_NOT_CONFIRMED: {', '.join(missing_selected)}")

    excluded = {normalize(fqn) for fqn in exclude or []}
    missing_excluded = sorted(excluded - set(by_fqn))
    if missing_excluded:
        raise ValueError(f"TABLE_NOT_CONFIRMED: {', '.join(missing_excluded)}")

    predicates = _parse_filters(filters)
    selected_after_exclude = [fqn for fqn in selected_fqns if fqn not in excluded]
    filter_not_selected = sorted(set(predicates) - set(selected_after_exclude))
    if filter_not_selected:
        raise ValueError(f"FILTER_TABLE_NOT_SELECTED: {', '.join(filter_not_selected)}")

    selected_plans: list[SourceTablePlan] = []
    for fqn in selected_after_exclude:
        plan = by_fqn[fqn]
        selected_plans.append(
            SourceTablePlan(
                fqn=plan.fqn,
                source_schema=plan.source_schema,
                source_table=plan.source_table,
                target_schema=plan.target_schema,
                target_table=plan.target_table,
                columns=plan.columns,
                predicate=predicates.get(fqn),
            )
        )
    return selected_plans


def _result_from_plan(plan: SourceTablePlan, *, status: str, rows_copied: int = 0, error: str | None = None) -> ReplicateTableResult:
    return ReplicateTableResult(
        fqn=plan.fqn,
        source_schema=plan.source_schema,
        source_table=plan.source_table,
        target_schema=plan.target_schema,
        target_table=plan.target_table,
        columns=plan.columns,
        predicate=plan.predicate,
        status=status,
        rows_copied=rows_copied,
        error=error,
    )


def run_replicate_source_tables(
    project_root: Path,
    *,
    limit: int | None,
    select: list[str] | None = None,
    exclude: list[str] | None = None,
    filters: list[str] | None = None,
    dry_run: bool = False,
    source_adapter: ReplicationAdapter | None = None,
    target_adapter: ReplicationAdapter | None = None,
) -> ReplicateSourceTablesOutput:
    """Copy capped rows from configured source tables into target source tables."""
    row_limit = _validate_limit(limit)
    plans = _select_tables(
        _load_confirmed_source_tables(project_root),
        select=select,
        exclude=exclude,
        filters=filters,
    )
    if dry_run:
        return ReplicateSourceTablesOutput(
            status="ok",
            dry_run=True,
            limit=row_limit,
            tables=[_result_from_plan(plan, status="planned") for plan in plans],
        )

    source_role = _require_runtime_role(project_root, "source")
    target_role = _require_runtime_role(project_root, "target")
    source = source_adapter or get_dbops(source_role.technology).from_role(source_role, project_root=project_root)
    target = target_adapter or get_dbops(target_role.technology).from_role(target_role, project_root=project_root)

    results: list[ReplicateTableResult] = []
    for plan in plans:
        try:
            columns, rows = source.fetch_source_rows(
                plan.source_schema,
                plan.source_table,
                limit=row_limit,
                predicate=plan.predicate,
                columns=plan.columns,
            )
            target.truncate_table(plan.target_schema, plan.target_table)
            copied = target.insert_rows(plan.target_schema, plan.target_table, columns, rows)
        except Exception as exc:
            logger.warning(
                "event=replicate_source_table status=failure table=%s error_type=%s",
                plan.fqn,
                type(exc).__name__,
            )
            results.append(_result_from_plan(plan, status="error", error=str(exc)))
            continue
        results.append(_result_from_plan(plan, status="ok", rows_copied=copied))

    status = "error" if any(result.status == "error" for result in results) else "ok"
    return ReplicateSourceTablesOutput(
        status=status,
        dry_run=False,
        limit=row_limit,
        tables=results,
    )
