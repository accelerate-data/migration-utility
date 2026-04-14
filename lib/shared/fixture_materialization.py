"""Shared orchestration for per-technology MigrationTest materialization."""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from shared.dbops import get_dbops
from shared.runtime_config_models import RuntimeRole

logger = logging.getLogger(__name__)


def _log_context(role: RuntimeRole) -> str:
    connection = role.connection
    parts = [f"technology={role.technology}"]
    if connection.database:
        parts.append(f"database={connection.database}")
    if connection.service:
        parts.append(f"service={connection.service}")
    if connection.schema_name:
        parts.append(f"schema={connection.schema_name}")
    return " ".join(parts)


def materialize_migration_test(
    role: RuntimeRole,
    repo_root: Path,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Materialize the canonical MigrationTest fixture for a runtime role.

    The technology-specific DB-ops adapter owns the env contract and the
    location of the repo-managed materialization script. This helper owns the
    orchestration of invoking that script with a fully resolved runtime role.
    """

    adapter = get_dbops(role.technology).from_role(role, project_root=repo_root)
    script_path = adapter.fixture_script_path(repo_root)
    if not script_path.exists():
        logger.error(
            "event=fixture_materialization_finish %s status=failure reason=script_missing script_path=%s",
            _log_context(role),
            script_path,
        )
        raise FileNotFoundError(f"MigrationTest fixture script not found: {script_path}")

    env = os.environ.copy()
    env.update(adapter.materialize_migration_test_env())
    if extra_env:
        env.update(extra_env)

    logger.info(
        "event=fixture_materialization_start %s script_path=%s",
        _log_context(role),
        script_path,
    )

    result = subprocess.run(
        [str(script_path)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    logger.info(
        "event=fixture_materialization_finish %s status=%s returncode=%s script_path=%s",
        _log_context(role),
        "success" if result.returncode == 0 else "failure",
        result.returncode,
        script_path,
    )
    return result
