"""dbt compile/build command helpers for target setup."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

MISSING_DBT_REMEDIATION = (
    "dbt executable not found in the installed ad-migration runtime. Run "
    "`ad-migration doctor drivers --project-root . --json`; if dbt is still "
    "missing, fix the public CLI package or Homebrew formula resources so the "
    "required dbt modules are bundled with the installed ad-migration runtime."
)


@dataclass(frozen=True)
class DbtCommandResult:
    """Outcome of invoking a dbt validation command."""

    ran: bool
    command: list[str]


def dbt_base_command(project_root: Path, subcommand: str) -> list[str]:
    dbt_root = project_root / "dbt"
    return [
        "dbt",
        subcommand,
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
    ]


def run_dbt_validation_command(
    project_root: Path,
    subcommand: str,
    selectors: list[str],
    *,
    exclude: list[str] | None = None,
) -> DbtCommandResult:
    if not selectors:
        return DbtCommandResult(ran=False, command=[])

    dbt_root = project_root / "dbt"
    command = [*dbt_base_command(project_root, subcommand), "--select", *selectors]
    if exclude:
        command.extend(["--exclude", *exclude])
    try:
        completed = subprocess.run(
            command,
            cwd=dbt_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ValueError(MISSING_DBT_REMEDIATION) from exc
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()
        message = f"dbt {subcommand} failed while validating generated staging models"
        if details:
            message = f"{message}: {details}"
        raise ValueError(message)

    logger.info(
        "event=dbt_validation_complete component=target_setup command=%s selectors=%d status=success",
        subcommand,
        len(selectors),
    )
    return DbtCommandResult(ran=True, command=command)
