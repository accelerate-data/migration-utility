"""Shared orchestration for per-technology MigrationTest materialization."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from shared.dbops import get_dbops
from shared.runtime_config_models import RuntimeRole


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
        raise FileNotFoundError(f"MigrationTest fixture script not found: {script_path}")

    env = os.environ.copy()
    env.update(adapter.materialize_migration_test_env())
    if extra_env:
        env.update(extra_env)

    return subprocess.run(
        [str(script_path)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
