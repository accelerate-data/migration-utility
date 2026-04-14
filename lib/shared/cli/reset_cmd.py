"""reset command — stub (implemented in Task 5)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import typer

logger = logging.getLogger(__name__)


def reset(
    scope: str = typer.Argument(..., help="Pipeline scope to reset (scope|profile|generate-tests|refactor)"),
    fqns: List[str] = typer.Argument(default=None, help="Fully-qualified table names"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")
    raise NotImplementedError("reset not yet implemented")
