"""dry_run.py — Migration stage prerequisite checker and content reader.

Standalone CLI with two subcommands:

    dry-run   Check guards for a (table, stage) pair and return
              eligibility + catalog/dbt content as JSON.

    guard     Check guards only (no content collection). Returns
              pass/fail JSON for use by skills and plugin commands.

Designed for consumption by the /status plugin command which adds LLM
reasoning on top of the deterministic output.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success (guards_passed field indicates pass/fail)
    1  domain failure (invalid stage, bad table FQN)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import typer

from shared.dry_run_content import _CONTENT_COLLECTORS
from shared.env_config import resolve_project_root
from shared.guards import run_guards  # re-exported for callers using dry_run.run_guards
from shared.name_resolver import fqn_parts, normalize  # re-exported for test compat

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Orchestrator ─────────────────────────────────────────────────────────────


def run_dry_run(
    project_root: Path,
    table_fqn: str,
    stage: str,
    detail: bool = False,
) -> dict[str, Any]:
    """Run dry-run checks for a (table, stage) pair.

    Returns a dict matching schemas/dry_run_output.json.
    """
    norm = normalize(table_fqn)
    guards_passed, guard_results = run_guards(project_root, norm, stage)

    result: dict[str, Any] = {
        "table": norm,
        "stage": stage,
        "guards_passed": guards_passed,
        "guard_results": guard_results,
    }

    if guards_passed:
        mode = "detail" if detail else "summary"
        collector = _CONTENT_COLLECTORS[stage][mode]
        result["content"] = collector(project_root, norm)

    return result


# ── CLI ──────────────────────────────────────────────────────────────────────


class Stage(str, Enum):
    scope = "scope"
    profile = "profile"
    test_gen = "test-gen"
    refactor = "refactor"
    migrate = "migrate"


class GuardStage(str, Enum):
    """Stages accepted by the guard subcommand.

    Includes pipeline stages plus skill-specific guard sets.
    """
    scope = "scope"
    profile = "profile"
    test_gen = "test-gen"
    refactor = "refactor"
    migrate = "migrate"
    generating_model = "generating-model"
    reviewing_model = "reviewing-model"
    reviewing_tests = "reviewing-tests"
    refactoring_sql = "refactoring-sql"
    setup_ddl = "setup-ddl"


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


@app.command("dry-run")
def dry_run_cmd(
    table: str = typer.Argument(..., help="Fully-qualified table name (schema.Name)"),
    stage: Stage = typer.Argument(..., help="Migration stage to check"),
    detail: bool = typer.Option(False, "--detail", help="Include full content blobs"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Check prerequisites for a migration stage and return eligibility + content."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        _emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    result = run_dry_run(root, table, stage.value, detail=detail)
    _emit(result)


@app.command("guard")
def guard_cmd(
    table: str = typer.Argument(..., help="Fully-qualified table name (schema.Name)"),
    stage: GuardStage = typer.Argument(..., help="Stage or skill name to check guards for"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root", help="Project root directory",
    ),
) -> None:
    """Check guards only (no content collection). Returns pass/fail JSON."""
    try:
        root = resolve_project_root(project_root)
    except RuntimeError as exc:
        logger.error("event=project_root_error error=%s", exc)
        _emit({"error": str(exc)})
        raise typer.Exit(code=2) from exc

    norm = normalize(table)
    guards_passed, guard_results = run_guards(root, norm, stage.value)
    _emit({
        "table": norm,
        "stage": stage.value,
        "passed": guards_passed,
        "guard_results": guard_results,
    })
