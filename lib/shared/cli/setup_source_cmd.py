"""setup-source command — stub (implemented in Task 2)."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def setup_source(
    technology: str = typer.Option(..., "--technology", help="Source technology: sql_server|oracle"),
    schemas: str = typer.Option(..., "--schemas", help="Comma-separated schemas to extract (e.g. silver,gold)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after extraction"),
) -> None:
    """Extract DDL and build the local catalog from a live source database."""
    raise NotImplementedError("setup-source not yet implemented")
