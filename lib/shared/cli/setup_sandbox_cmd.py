"""setup-sandbox command — stub (implemented in Task 4)."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer


def setup_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Provision sandbox schema from manifest runtime.sandbox configuration."""
    raise NotImplementedError("setup-sandbox not yet implemented")
