"""add-source-table command — stub (implemented in Task 5)."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer


def add_source_table(
    fqns: List[str] = typer.Argument(..., help="One or more fully-qualified table names to add"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after update"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Add one or more source tables to the migration catalog."""
    raise NotImplementedError("add-source-table not yet implemented")
