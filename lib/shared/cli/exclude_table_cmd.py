"""exclude-table command — stub (implemented in Task 5)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import typer

logger = logging.getLogger(__name__)


def exclude_table(
    fqns: List[str] = typer.Argument(default=None, help="One or more fully-qualified table names to exclude"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after update"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Mark one or more source tables as excluded from migration."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")
    raise NotImplementedError("exclude-table not yet implemented")
