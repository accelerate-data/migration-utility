"""teardown-sandbox command — stub (implemented in Task 4)."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

logger = logging.getLogger(__name__)


def teardown_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Tear down sandbox schema from manifest runtime.sandbox configuration."""
    raise NotImplementedError("teardown-sandbox not yet implemented")
