"""Top-level ad-migration Typer app."""
import logging

import typer

logger = logging.getLogger(__name__)

from shared.cli.add_source_table_cmd import add_source_table
from shared.cli.exclude_table_cmd import exclude_table
from shared.cli.reset_cmd import reset
from shared.cli.setup_sandbox_cmd import setup_sandbox
from shared.cli.setup_source_cmd import setup_source
from shared.cli.setup_target_cmd import setup_target
from shared.cli.teardown_sandbox_cmd import teardown_sandbox

app = typer.Typer(
    name="ad-migration",
    help="Migration pipeline CLI for warehouses to dbt.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

app.command("setup-source")(setup_source)
app.command("setup-target")(setup_target)
app.command("setup-sandbox")(setup_sandbox)
app.command("teardown-sandbox")(teardown_sandbox)
app.command("reset")(reset)
app.command("exclude-table")(exclude_table)
app.command("add-source-table")(add_source_table)
