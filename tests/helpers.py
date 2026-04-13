from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SHARED_LIB_DIR = REPO_ROOT / "plugin" / "lib"


def git_init(path: Path) -> None:
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)


def run_python_module(
    module: str,
    args: list[str],
    *,
    cwd: Path = SHARED_LIB_DIR,
    timeout: int = 30,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def run_setup_ddl_cli(args: list[str], *, timeout: int = 30) -> subprocess.CompletedProcess:
    return run_python_module("shared.setup_ddl", args, timeout=timeout)


def run_catalog_enrich_cli(
    project_root: Path,
    extra_args: list[str] | tuple[str, ...] = (),
    *,
    timeout: int = 30,
) -> subprocess.CompletedProcess:
    return run_python_module(
        "shared.catalog_enrich",
        ["--project-root", str(project_root), *extra_args],
        timeout=timeout,
    )
