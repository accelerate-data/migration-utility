"""dbt seed command execution helpers."""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

from shared.target_setup_support.dbt_commands import MISSING_DBT_REMEDIATION, dbt_base_command

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbtSeedResult:
    """Outcome of invoking dbt seed for exported seed CSV files."""

    ran: bool
    command: list[str]


def materialize_seed_tables(project_root: Path, seed_files: list[str]) -> DbtSeedResult:
    """Run dbt seed so exported seed CSVs are materialized in the target schema."""
    seed_csv_files = [seed_file for seed_file in seed_files if seed_file.endswith(".csv")]
    if not seed_csv_files:
        return DbtSeedResult(ran=False, command=[])

    dbt_root = project_root / "dbt"
    command = dbt_base_command(project_root, "seed")
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
        message = "dbt seed failed while materializing seed tables"
        if details:
            message = f"{message}: {details}"
        raise ValueError(message)

    logger.info(
        "event=dbt_seed_complete component=target_setup seed_files=%d status=success",
        len(seed_csv_files),
    )
    return DbtSeedResult(ran=True, command=command)
