"""Rich formatting helpers for ad-migration CLI commands."""
from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)

console = Console()
err_console = Console(stderr=True)

_quiet: bool = False
_verbose: bool = False


def set_quiet(value: bool) -> None:
    global _quiet
    _quiet = value


def set_verbose(value: bool) -> None:
    global _verbose
    _verbose = value


def is_verbose() -> bool:
    return _verbose


def success(message: str) -> None:
    if not _quiet:
        console.print(f"[green]✓[/green] {message}")


def warn(message: str) -> None:
    if not _quiet:
        err_console.print(f"[yellow]![/yellow] {message}")


def error(message: str) -> None:
    err_console.print(f"[red]✗[/red] {message}")


def print_table(title: str, rows: list[tuple[str, str]], columns: tuple[str, str] = ("Item", "Status")) -> None:
    if _quiet:
        return
    table = Table(title=title, show_header=True, header_style="bold")
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    console.print(table)


def summarize_repo_mutations(paths: Iterable[str | Path]) -> list[str]:
    """Return stable human-readable path summaries for repo mutations."""
    root_paths: list[str] = []
    root_dir_paths: list[str] = []
    grouped_paths: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()

    for raw_path in paths:
        raw_text = str(raw_path).strip()
        path = Path(raw_text)
        if str(path) in ("", "."):
            continue
        path_str = path.as_posix()
        if raw_text.endswith("/") and not path_str.endswith("/"):
            path_str += "/"
        if path_str in seen:
            continue
        seen.add(path_str)

        parent = path.parent.as_posix()
        if parent in ("", "."):
            if path_str.endswith("/"):
                root_dir_paths.append(path_str)
            else:
                root_paths.append(path_str)
        else:
            grouped_paths[parent].append(path_str)

    summaries: list[str] = [*root_paths, *root_dir_paths]
    for parent in sorted(grouped_paths):
        child_paths = sorted(grouped_paths[parent])
        if len(child_paths) > 3:
            summaries.append(f"{parent}/ - {len(child_paths)} files")
        else:
            summaries.extend(child_paths)
    return summaries


def remind_review_and_commit(paths: Iterable[str | Path] | None = None) -> None:
    if _quiet:
        return
    summaries = summarize_repo_mutations(paths or [])
    if not summaries:
        return
    console.print("\n[bold]Updated repo state:[/bold]")
    for summary in summaries:
        console.print(f"  {summary}")
    console.print("\n[bold]Next step:[/bold] Review and commit the repo changes before continuing.")
