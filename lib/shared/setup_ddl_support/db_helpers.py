from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any


def build_schema_in_clause(schemas: list[str], *, uppercase: bool = False) -> str:
    """Build a SQL IN clause literal from validated schema names."""
    values: list[str] = []
    for schema in schemas:
        if "'" in schema or ";" in schema:
            raise ValueError(f"Invalid schema name: {schema!r}")
        value = schema.upper() if uppercase else schema
        values.append(f"'{value}'")
    return ", ".join(values)


def write_staging_json(
    staging_dir: Path,
    filename: str,
    rows: list[Any],
    *,
    logger: logging.Logger,
    event: str,
) -> None:
    """Write staging rows as JSON and log a per-file completion event."""
    path = staging_dir / filename
    path.write_text(json.dumps(rows, default=str), encoding="utf-8")
    logger.info("%s file=%s rows=%d", event, filename, len(rows))
