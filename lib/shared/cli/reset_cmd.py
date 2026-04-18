"""reset command — reset pipeline state for a given stage and objects."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.dry_run_core import RESET_GLOBAL_MANIFEST_SECTIONS, RESET_GLOBAL_PATHS, RESETTABLE_STAGES, run_reset_migration
from shared.dry_run_support.common import RESET_PRESERVE_CATALOG_PATHS
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, remind_review_and_commit, success, warn
from shared.loader_io import clear_manifest_sandbox
from shared.runtime_config import get_sandbox_name
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)

_GLOBAL_BLAST_RADIUS = (
    "Directories:  " + ", ".join(RESET_GLOBAL_PATHS) + "\n"
    "Manifest:     " + ", ".join(RESET_GLOBAL_MANIFEST_SECTIONS)
)
_PRESERVE_CATALOG_BLAST_RADIUS = (
    "Directories:      " + ", ".join(RESET_PRESERVE_CATALOG_PATHS) + "\n"
    "Catalog sections: table.test_gen, table.generate, table.refactor, "
    "view.test_gen, view.generate, view.refactor, procedure.refactor"
)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any], project_root: Path | None = None) -> Any:
    return _th_create_backend(manifest, project_root)


def _get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    return get_sandbox_name(manifest)


def _manual_cleanup_instructions(sandbox_db: str) -> str:
    return (
        f"  SQL Server:  DROP DATABASE [{sandbox_db}]\n"
        f"  Oracle:      DROP USER {sandbox_db} CASCADE"
    )


def _teardown_sandbox_if_configured(root: Path) -> None:
    """Tear down sandbox if configured. On failure, warns and continues — never blocks the reset."""
    try:
        manifest = _load_manifest(root)
    except Exception:
        return

    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        return

    console.print(f"Sandbox configured: [bold]{sandbox_db}[/bold] — tearing down first...")
    logger.info("event=global_reset_sandbox_teardown component=reset_cmd sandbox=%s", sandbox_db)

    teardown_ok = False
    try:
        backend = _create_backend(manifest, root)
        with cli_error_handler("tearing down sandbox database"):
            result = backend.sandbox_down(sandbox_db)
        teardown_ok = result.status == "ok"
        if not teardown_ok:
            logger.warning(
                "event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s status=%s",
                sandbox_db, result.status,
            )
    except typer.Exit:
        # cli_error_handler raised Exit — treat as teardown failure and continue
        pass
    except Exception as exc:
        logger.warning(
            "event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s error=%s",
            sandbox_db, exc,
        )

    if not teardown_ok:
        warn(
            f"Sandbox teardown failed for [bold]{sandbox_db}[/bold] — continuing reset.\n"
            f"Clean up the database manually:\n{_manual_cleanup_instructions(sandbox_db)}"
        )

    clear_manifest_sandbox(root)
    if teardown_ok:
        logger.info("event=global_reset_sandbox_teardown_ok component=reset_cmd sandbox=%s", sandbox_db)


def reset(
    stage: str = typer.Argument(..., help="Pipeline stage to reset (scope|profile|generate-tests|refactor|all)"),
    fqns: list[str] = typer.Argument(default=None, help="Fully-qualified table names (not used for 'all')"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    preserve_catalog: bool = typer.Option(
        False,
        "--preserve-catalog",
        help="For reset all, preserve catalog/DDL and clear generated target state only.",
    ),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects.

    Use 'all' to wipe the full migration state and return the project to a
    clean post-init state ready for setup-source. Any configured sandbox is
    torn down first.
    """
    root = project_root if project_root is not None else Path.cwd()

    if stage == "all":
        if fqns:
            error("Global reset ('all') does not accept table arguments.")
            raise typer.Exit(code=1)

        if not yes:
            if preserve_catalog:
                console.print(
                    "[bold red]Preserve-catalog reset[/bold red] will permanently clear:\n"
                    f"{_PRESERVE_CATALOG_BLAST_RADIUS}"
                )
            else:
                console.print(f"[bold red]Global reset[/bold red] will permanently delete:\n{_GLOBAL_BLAST_RADIUS}")
            confirmed = typer.confirm("This cannot be undone. Continue?", default=False)
            if not confirmed:
                console.print("Aborted.")
                return

        logger.info(
            "event=global_reset_start component=reset_cmd stage=all preserve_catalog=%s",
            preserve_catalog,
        )

        _teardown_sandbox_if_configured(root)

        if preserve_catalog:
            result = run_reset_migration(root, "all", [], preserve_catalog=True)
        else:
            result = run_reset_migration(root, "all", [])

        logger.info(
            "event=global_reset_complete component=reset_cmd deleted_paths=%s missing_paths=%s "
            "cleared_manifest=%s cleared_catalog=%s preserve_catalog=%s",
            result.deleted_paths,
            result.missing_paths,
            result.cleared_manifest_sections,
            result.cleared_catalog_sections,
            preserve_catalog,
        )

        print_table(
            "Preserve-catalog Reset Summary" if preserve_catalog else "Global Reset Summary",
            [
                ("Deleted", ", ".join(result.deleted_paths) if result.deleted_paths else "—"),
                ("Missing", ", ".join(result.missing_paths) if result.missing_paths else "—"),
                ("Manifest cleared", ", ".join(result.cleared_manifest_sections) if result.cleared_manifest_sections else "—"),
                ("Catalog cleared", ", ".join(result.cleared_catalog_sections) if result.cleared_catalog_sections else "—"),
            ],
            columns=("", ""),
        )
        if preserve_catalog:
            success("Generated target state cleared. Run setup-target to recreate the dbt project.")
            review_paths = [
                *[f"{path}/" for path in result.deleted_paths],
                *[entry.split(":", 1)[0] for entry in result.cleared_catalog_sections],
            ]
            remind_review_and_commit(review_paths)
        else:
            success("Project reset to post-init state. Run setup-source to restart the pipeline.")
            deleted_paths = [f"{path}/" for path in result.deleted_paths]
            remind_review_and_commit([*deleted_paths, "manifest.json"])
        return

    if preserve_catalog:
        error("--preserve-catalog is only supported with reset all.")
        raise typer.Exit(code=1)

    if stage not in RESETTABLE_STAGES:
        error(f"Invalid stage {stage!r}. Must be one of: {', '.join(sorted(RESETTABLE_STAGES))} or 'all'")
        raise typer.Exit(code=1)

    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    if not yes:
        fqn_list = ", ".join(fqns)
        confirmed = typer.confirm(f"Reset stage '{stage}' for: {fqn_list}?", default=False)
        if not confirmed:
            console.print("Aborted.")
            return

    logger.info("event=reset_start component=reset_cmd stage=%s fqns=%s", stage, fqns)

    result = run_reset_migration(root, stage, list(fqns))

    logger.info(
        "event=reset_complete component=reset_cmd stage=%s reset=%s noop=%s blocked=%s not_found=%s",
        stage, result.reset, result.noop, result.blocked, result.not_found,
    )

    print_table(
        "Reset Summary",
        [
            ("Reset", str(len(result.reset))),
            ("No-op", str(len(result.noop))),
            ("Blocked", str(len(result.blocked))),
            ("Not found", str(len(result.not_found))),
        ],
        columns=("Category", "Count"),
    )
    if result.blocked:
        error(f"Blocked: {', '.join(result.blocked)}")
    if result.not_found:
        error(f"Not found: {', '.join(result.not_found)}")
    if result.blocked or result.not_found:
        raise typer.Exit(code=1)
    written_paths = [
        path
        for target in result.targets
        for path in [*target.mutated_files, *target.deleted_files]
    ]
    remind_review_and_commit(written_paths)
