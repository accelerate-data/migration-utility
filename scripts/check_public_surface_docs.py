#!/usr/bin/env python3
"""Require wiki updates when a PR adds public commands, skills, or CLI commands."""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


CLI_MAIN_PATH = "lib/shared/cli/main.py"
WIKI_PREFIX = "docs/wiki/"


@dataclass(frozen=True)
class ChangedPath:
    status: str
    path: str


@dataclass(frozen=True)
class PublicSurfaceDocsAudit:
    public_changes: list[str]
    wiki_changes: list[str]

    @property
    def ok(self) -> bool:
        return not self.public_changes or bool(self.wiki_changes)


def _is_new_root_command(change: ChangedPath) -> bool:
    path = Path(change.path)
    return (
        change.status in {"A", "R"}
        and len(path.parts) == 2
        and path.parts[0] == "commands"
        and path.suffix == ".md"
    )


def _is_new_public_skill(change: ChangedPath) -> bool:
    path = Path(change.path)
    return (
        change.status in {"A", "R"}
        and len(path.parts) == 3
        and path.parts[0] == "skills"
        and not path.parts[1].startswith("_")
        and path.parts[2] == "SKILL.md"
    )


def _is_new_cli_module(change: ChangedPath) -> bool:
    path = Path(change.path)
    return (
        change.status in {"A", "R"}
        and len(path.parts) == 4
        and path.parts[:3] == ("lib", "shared", "cli")
        and path.name.endswith("_cmd.py")
    )


def _adds_cli_registration(cli_main_patch: str) -> bool:
    registration_markers = (
        "+app.command(",
        "+doctor_app.command(",
        "+app.add_typer(",
    )
    return any(marker in cli_main_patch for marker in registration_markers)


def audit_public_surface_docs(
    changes: list[ChangedPath],
    cli_main_patch: str,
) -> PublicSurfaceDocsAudit:
    public_changes = [
        change.path
        for change in changes
        if _is_new_root_command(change)
        or _is_new_public_skill(change)
        or _is_new_cli_module(change)
    ]
    if _adds_cli_registration(cli_main_patch) and CLI_MAIN_PATH not in public_changes:
        public_changes.append(CLI_MAIN_PATH)

    wiki_changes = [change.path for change in changes if change.path.startswith(WIKI_PREFIX)]

    return PublicSurfaceDocsAudit(
        public_changes=sorted(public_changes),
        wiki_changes=sorted(wiki_changes),
    )


def _git_diff_name_status(base: str, head: str) -> list[ChangedPath]:
    output = subprocess.check_output(
        ["git", "diff", "--name-status", "--diff-filter=AMR", base, head],
        text=True,
    )
    changes: list[ChangedPath] = []
    for line in output.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        status = parts[0][0]
        path = parts[-1]
        changes.append(ChangedPath(status, path))
    return changes


def _git_diff_patch(base: str, head: str, path: str) -> str:
    return subprocess.check_output(
        ["git", "diff", "--unified=0", base, head, "--", path],
        text=True,
    )


def _print_result(result: PublicSurfaceDocsAudit) -> None:
    print(f"public_changes: {result.public_changes}")
    print(f"wiki_changes: {result.wiki_changes}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="Base git revision.")
    parser.add_argument("--head", required=True, help="Head git revision.")
    args = parser.parse_args(argv)

    changes = _git_diff_name_status(args.base, args.head)
    cli_main_patch = _git_diff_patch(args.base, args.head, CLI_MAIN_PATH)
    result = audit_public_surface_docs(changes, cli_main_patch)
    _print_result(result)

    if result.ok:
        return 0

    print(
        "::error::Public CLI, slash-command, or skill surface changed without a docs/wiki update."
    )
    print("Update docs/wiki so the published wiki stays aligned with public entrypoints.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
