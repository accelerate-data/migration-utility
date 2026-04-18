"""Top-level ad-migration Typer app."""
from __future__ import annotations

import importlib.metadata
import logging
import sys
from pathlib import Path

import tomllib

import typer

from shared.cli import output
from shared.cli.add_seed_table_cmd import add_seed_table
from shared.cli.add_source_table_cmd import add_source_table
from shared.cli.doctor_drivers_cmd import drivers as doctor_drivers
from shared.cli.exclude_table_cmd import exclude_table
from shared.cli.reset_cmd import reset
from shared.cli.replicate_source_tables_cmd import replicate_source_tables
from shared.cli.setup_sandbox_cmd import setup_sandbox
from shared.cli.setup_source_cmd import setup_source
from shared.cli.setup_target_cmd import setup_target
from shared.cli.teardown_sandbox_cmd import teardown_sandbox

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="ad-migration",
    help="Migration pipeline CLI for warehouses to dbt.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
doctor_app = typer.Typer(
    name="doctor",
    help="Diagnose the installed ad-migration runtime.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)


def _fallback_pyproject_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    if pyproject_path.exists():
        with pyproject_path.open("rb") as handle:
            data = tomllib.load(handle)
        return data["project"]["version"]
    raise importlib.metadata.PackageNotFoundError("ad-migration-cli")


def _package_version() -> str:
    for distribution_name in ("ad-migration-cli", "ad-migration-shared", "ad-migration"):
        try:
            return importlib.metadata.version(distribution_name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return _fallback_pyproject_version()


def _print_version(value: bool) -> None:
    if not value:
        return
    typer.echo(_package_version())
    raise typer.Exit()


@app.callback()
def _main(
    quiet: bool = typer.Option(False, "--quiet", help="Suppress all output except errors, for CI use."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show warnings and log output on stderr."),
    version: bool = typer.Option(
        False,
        "--version",
        callback=_print_version,
        is_eager=True,
        help="Show the ad-migration CLI version and exit.",
    ),
) -> None:
    if quiet:
        output.set_quiet(True)
    if verbose:
        output.set_verbose(True)
        _handler = logging.StreamHandler(sys.stderr)
        _handler.setLevel(logging.WARNING)
        _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        logging.getLogger().addHandler(_handler)
        logging.captureWarnings(True)
        logging.getLogger("sqlglot").setLevel(logging.WARNING)
    else:
        # sqlglot prints parse-ambiguity messages directly through its logger at
        # WARNING level — suppress them so they don't bleed into CLI output.
        logging.getLogger("sqlglot").setLevel(logging.CRITICAL)


app.command("setup-source")(setup_source)
app.command("setup-target")(setup_target)
app.command("setup-sandbox")(setup_sandbox)
app.command("teardown-sandbox")(teardown_sandbox)
app.command("reset")(reset)
app.command("replicate-source-tables")(replicate_source_tables)
app.command("exclude-table")(exclude_table)
app.command("add-source-table")(add_source_table)
app.command("add-seed-table")(add_seed_table)
doctor_app.command("drivers")(doctor_drivers)
app.add_typer(doctor_app)
