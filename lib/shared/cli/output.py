"""Rich formatting helpers for ad-migration CLI commands."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def success(message: str) -> None:
    console.print(f"[green]✓[/green] {message}")


def warn(message: str) -> None:
    err_console.print(f"[yellow]![/yellow] {message}")


def error(message: str) -> None:
    err_console.print(f"[red]✗[/red] {message}")


def print_table(title: str, rows: list[tuple[str, str]], columns: tuple[str, str] = ("Item", "Status")) -> None:
    table = Table(title=title, show_header=True, header_style="bold")
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    console.print(table)


@contextmanager
def spinner(message: str) -> Iterator[None]:
    with Progress(SpinnerColumn(), TextColumn(message), transient=True) as progress:
        progress.add_task("", total=None)
        yield
