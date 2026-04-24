#!/usr/bin/env python3
"""Ensure repo-local dependency environments are fresh for a given repo root."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


STAMP_VERSION = 1
LIB_VERIFY_IMPORTS = (
    "import pyodbc, oracledb, dbt.adapters.oracle, dbt.adapters.sqlserver"
)


@dataclass(frozen=True)
class EnvSpec:
    key: str
    label: str
    relative_dir: str
    fingerprint_files: tuple[str, ...]
    env_dir_name: str
    stamp_name: str
    sync_command: tuple[str, ...]
    sync_failure_code: int
    sync_failure_message: str
    verify_command: tuple[str, ...] | None = None
    verify_failure_code: int | None = None
    verify_failure_message: str | None = None


ENV_SPECS: dict[str, EnvSpec] = {
    "lib": EnvSpec(
        key="lib",
        label="lib",
        relative_dir="lib",
        fingerprint_files=("pyproject.toml", "uv.lock"),
        env_dir_name=".venv",
        stamp_name=".bootstrap-fingerprint.json",
        sync_command=("uv", "sync", "--extra", "dev"),
        sync_failure_code=10,
        sync_failure_message="uv sync failed for lib.",
        verify_command=("uv", "run", "python", "-c", LIB_VERIFY_IMPORTS),
        verify_failure_code=11,
        verify_failure_message=(
            "The lib environment does not import pyodbc, oracledb, "
            "dbt.adapters.oracle, and dbt.adapters.sqlserver."
        ),
    ),
    "mcp_ddl": EnvSpec(
        key="mcp_ddl",
        label="mcp/ddl",
        relative_dir="mcp/ddl",
        fingerprint_files=("pyproject.toml", "uv.lock"),
        env_dir_name=".venv",
        stamp_name=".bootstrap-fingerprint.json",
        sync_command=("uv", "sync"),
        sync_failure_code=20,
        sync_failure_message="uv sync failed for mcp/ddl.",
    ),
    "tests_evals": EnvSpec(
        key="tests_evals",
        label="tests/evals",
        relative_dir="tests/evals",
        fingerprint_files=("package.json", "package-lock.json"),
        env_dir_name="node_modules",
        stamp_name=".bootstrap-fingerprint.json",
        sync_command=("npm", "ci", "--no-audit", "--no-fund"),
        sync_failure_code=30,
        sync_failure_message="npm dependency bootstrap failed for tests/evals.",
    ),
}


class BootstrapFailure(Exception):
    def __init__(self, exit_code: int, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.message = message


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ensure repo-local dependency environments are bootstrapped and fresh."
    )
    parser.add_argument(
        "--repo-root",
        default=Path(__file__).resolve().parents[1],
        type=Path,
        help="Repo root whose local environments should be managed.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-error output.",
    )
    parser.add_argument(
        "command",
        choices=("ensure",),
        help="Operation to perform.",
    )
    parser.add_argument(
        "target",
        choices=("all", "lib", "mcp_ddl", "tests_evals"),
        help="Environment target to manage.",
    )
    return parser.parse_args()


def fingerprint_for(spec: EnvSpec, repo_root: Path) -> str:
    digest = hashlib.sha256()
    digest.update(f"stamp-version:{STAMP_VERSION}\n".encode("utf-8"))
    digest.update(f"target:{spec.key}\n".encode("utf-8"))
    digest.update(f"sync:{' '.join(spec.sync_command)}\n".encode("utf-8"))
    if spec.verify_command:
        digest.update(f"verify:{' '.join(spec.verify_command)}\n".encode("utf-8"))
    env_root = repo_root / spec.relative_dir
    for relative_file in spec.fingerprint_files:
        file_path = env_root / relative_file
        digest.update(f"file:{relative_file}\n".encode("utf-8"))
        if file_path.exists():
            digest.update(b"present\n")
            digest.update(file_path.read_bytes())
        else:
            digest.update(b"missing\n")
    return digest.hexdigest()


def stamp_path_for(spec: EnvSpec, repo_root: Path) -> Path:
    return repo_root / spec.relative_dir / spec.env_dir_name / spec.stamp_name


def env_dir_for(spec: EnvSpec, repo_root: Path) -> Path:
    return repo_root / spec.relative_dir / spec.env_dir_name


def is_fresh(spec: EnvSpec, repo_root: Path, expected_fingerprint: str) -> bool:
    env_dir = env_dir_for(spec, repo_root)
    stamp_path = stamp_path_for(spec, repo_root)
    if not env_dir.exists() or not stamp_path.exists():
        return False
    try:
        payload = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return (
        payload.get("version") == STAMP_VERSION
        and payload.get("target") == spec.key
        and payload.get("fingerprint") == expected_fingerprint
    )


def run_command(
    command: tuple[str, ...], cwd: Path, failure_code: int, failure_message: str
) -> None:
    completed = subprocess.run(command, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise BootstrapFailure(failure_code, failure_message)


def resolve_sync_command(spec: EnvSpec, repo_root: Path) -> tuple[str, ...]:
    if spec.key != "tests_evals":
        return spec.sync_command
    lockfile = repo_root / spec.relative_dir / "package-lock.json"
    if lockfile.exists():
        return spec.sync_command
    return ("npm", "install", "--no-audit", "--no-fund")


def write_stamp(spec: EnvSpec, repo_root: Path, fingerprint: str) -> None:
    stamp_path = stamp_path_for(spec, repo_root)
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": STAMP_VERSION,
        "target": spec.key,
        "fingerprint": fingerprint,
    }
    stamp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def ensure_env(spec: EnvSpec, repo_root: Path, quiet: bool) -> None:
    env_root = repo_root / spec.relative_dir
    if not env_root.exists():
        if not quiet:
            print(f"{spec.label}: skipped (missing directory)")
        return
    fingerprint = fingerprint_for(spec, repo_root)
    if is_fresh(spec, repo_root, fingerprint):
        if not quiet:
            print(f"{spec.label}: fresh")
        return

    sync_command = resolve_sync_command(spec, repo_root)
    if not quiet:
        print(f"{spec.label}: syncing with {' '.join(sync_command)}")
    run_command(
        sync_command,
        cwd=env_root,
        failure_code=spec.sync_failure_code,
        failure_message=spec.sync_failure_message,
    )

    if spec.verify_command and spec.verify_failure_code and spec.verify_failure_message:
        if not quiet:
            print(f"{spec.label}: verifying dependencies")
        run_command(
            spec.verify_command,
            cwd=env_root,
            failure_code=spec.verify_failure_code,
            failure_message=spec.verify_failure_message,
        )

    write_stamp(spec, repo_root, fingerprint)
    if not quiet:
        print(f"{spec.label}: ready")


def ensure_targets(target: str, repo_root: Path, quiet: bool) -> None:
    targets = ENV_SPECS.values() if target == "all" else (ENV_SPECS[target],)
    for spec in targets:
        ensure_env(spec, repo_root, quiet)


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    try:
        ensure_targets(args.target, repo_root, args.quiet)
    except BootstrapFailure as exc:
        print(exc.message, file=sys.stderr)
        return exc.exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
