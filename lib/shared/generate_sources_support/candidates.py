"""Catalog candidate classification for dbt source generation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from shared.output_models.generate_sources import GenerateSourcesOutput

logger = logging.getLogger(__name__)


@dataclass
class SourceCandidates:
    """Classified catalog tables for sources.yml generation."""

    source_tables: list[dict[str, Any]] = field(default_factory=list)
    included: list[str] = field(default_factory=list)
    excluded: list[str] = field(default_factory=list)
    unconfirmed: list[str] = field(default_factory=list)
    incomplete: list[str] = field(default_factory=list)


def list_confirmed_source_tables_from_dir(tables_dir: Path) -> list[str]:
    """Return confirmed source-table FQNs from a catalog tables directory."""
    if not tables_dir.is_dir():
        return []

    included: list[str] = []
    for table_file in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(table_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                table_file,
            )
            continue

        if cat.get("excluded") or cat.get("is_source") is not True:
            continue

        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        included.append(f"{schema}.{name.lower()}")

    return included


def collect_source_candidates(tables_dir: Path) -> SourceCandidates:
    """Read catalog table files and classify source-generation candidates."""
    candidates = SourceCandidates()
    if not tables_dir.is_dir():
        return candidates

    for table_file in sorted(tables_dir.glob("*.json")):
        try:
            cat = json.loads(table_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning(
                "event=generate_sources_skip_file path=%s reason=parse_error",
                table_file,
            )
            continue

        schema = cat.get("schema", "").lower()
        name = cat.get("name", "")
        fqn = f"{schema}.{name.lower()}"

        scoping = cat.get("scoping") or {}
        status = scoping.get("status")

        if cat.get("excluded"):
            continue
        if cat.get("is_seed") is True:
            continue
        if cat.get("is_source") is True:
            candidates.included.append(fqn)
            candidates.source_tables.append(cat)
        elif status == "resolved":
            candidates.excluded.append(fqn)
        elif status == "no_writer_found":
            candidates.unconfirmed.append(fqn)
        else:
            candidates.incomplete.append(fqn)
    return candidates


def validate_source_namespace(candidates: SourceCandidates) -> GenerateSourcesOutput | None:
    """Ensure confirmed source table names are unique in the bronze namespace."""
    source_name_to_fqn: dict[str, str] = {}
    for cat in candidates.source_tables:
        schema_name = str(cat.get("schema", "")).lower()
        table_name = str(cat.get("name", ""))
        fqn = f"{schema_name}.{table_name.lower()}"
        source_table_name = table_name.lower()
        existing_fqn = source_name_to_fqn.get(source_table_name)
        if existing_fqn is not None and existing_fqn != fqn:
            message = (
                "Confirmed source tables must have unique names under the bronze "
                f"source namespace: {existing_fqn}, {fqn}"
            )
            logger.error(
                "event=generate_sources_duplicate_source_name table=%s existing=%s duplicate=%s",
                source_table_name,
                existing_fqn,
                fqn,
            )
            return GenerateSourcesOutput(
                sources=None,
                included=candidates.included,
                excluded=candidates.excluded,
                unconfirmed=candidates.unconfirmed,
                incomplete=candidates.incomplete,
                error="SOURCE_NAME_COLLISION",
                message=message,
            )
        source_name_to_fqn[source_table_name] = fqn
    return None


def _column_sql_type(column: dict[str, Any]) -> str | None:
    value = column.get("sql_type")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def validate_staging_contract_types(candidates: SourceCandidates) -> GenerateSourcesOutput | None:
    """Ensure generated staging contracts use target-normalized catalog types."""
    for cat in candidates.source_tables:
        schema_name = str(cat.get("schema", "")).lower()
        table_name = str(cat.get("name", ""))
        for column in cat.get("columns", []):
            if not isinstance(column, dict) or not column.get("name"):
                continue
            if _column_sql_type(column):
                continue
            column_name = str(column["name"])
            fqn = f"{schema_name}.{table_name}.{column_name}"
            message = (
                f"Cannot generate staging contract for {fqn}: catalog column is "
                "missing target-normalized sql_type"
            )
            logger.error(
                "event=generate_sources_staging_contract_type_missing table=%s column=%s",
                f"{schema_name}.{table_name}",
                column_name,
            )
            return GenerateSourcesOutput(
                sources=None,
                included=candidates.included,
                excluded=candidates.excluded,
                unconfirmed=candidates.unconfirmed,
                incomplete=candidates.incomplete,
                error="STAGING_CONTRACT_TYPE_MISSING",
                message=message,
            )
    return None
