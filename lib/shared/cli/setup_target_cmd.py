"""setup-target command — stub (implemented in Task 3)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

logger = logging.getLogger(__name__)


def setup_target(
    technology: str = typer.Option(..., "--technology", help="Target technology: fabric|snowflake|duckdb"),
    source_schema: Optional[str] = typer.Option(None, "--source-schema", help="Override source schema (default: bronze)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after scaffolding"),
) -> None:
    """Scaffold dbt target configuration for the chosen target platform."""
    raise NotImplementedError("setup-target not yet implemented")
