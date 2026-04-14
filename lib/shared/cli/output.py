"""Rich formatting helpers for ad-migration CLI commands."""
from __future__ import annotations

import logging

from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)

console = Console()
err_console = Console(stderr=True)

_quiet: bool = False


def set_quiet(value: bool) -> None:
    global _quiet
    _quiet = value


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


