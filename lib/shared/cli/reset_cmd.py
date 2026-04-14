"""reset command — stub (implemented in Task 5)."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer


def reset(
    scope: str = typer.Argument(..., help="Pipeline stage to reset: scope|profile|generate-tests|refactor"),
    fqns: List[str] = typer.Argument(..., help="One or more fully-qualified object names"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects."""
    raise NotImplementedError("reset not yet implemented")
