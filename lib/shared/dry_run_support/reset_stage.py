"""Staged dry-run reset behavior."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.catalog import detect_catalog_bucket, write_json
from shared.dry_run_support.common import RESETTABLE_STAGES, _RESET_STAGE_SECTIONS, read_catalog_json
from shared.dry_run_support.reset_files import delete_if_present
from shared.name_resolver import normalize
from shared.output_models.dry_run import ResetMigrationOutput, ResetTargetResult


def reset_table_sections(
    project_root: Path,
    norm: str,
    stage: str,
) -> tuple[list[str], list[str], list[str], str | None]:
    table_path = project_root / "catalog" / "tables" / f"{norm}.json"
    table_data = read_catalog_json(table_path)
    writer = (
        table_data.get("scoping", {}).get("selected_writer")
        if isinstance(table_data.get("scoping"), dict)
        else None
    )

    cleared_sections: list[str] = []
    deleted_files: list[str] = []
    mutated_files: list[str] = []

    for key in _RESET_STAGE_SECTIONS[stage]:
        if key in table_data:
            del table_data[key]
            cleared_sections.append(f"table.{key}")

    if stage in ("scope", "profile", "generate-tests"):
        spec_path = project_root / "test-specs" / f"{norm}.json"
        if delete_if_present(spec_path):
            deleted_files.append(f"test-specs/{norm}.json")

    write_json(table_path, table_data)
    if cleared_sections:
        mutated_files.append(str(table_path.relative_to(project_root)))
    return cleared_sections, deleted_files, mutated_files, writer


def reset_writer_refactor(
    project_root: Path,
    writer_fqn: str | None,
) -> tuple[list[str], list[str]]:
    if not writer_fqn:
        return [], []

    writer_norm = normalize(writer_fqn)
    proc_path = project_root / "catalog" / "procedures" / f"{writer_norm}.json"
    if not proc_path.exists():
        return [], []

    proc_data = read_catalog_json(proc_path)
    if "refactor" not in proc_data:
        return [], []

    del proc_data["refactor"]
    write_json(proc_path, proc_data)
    return [f"procedure:{writer_norm}.refactor"], [str(proc_path.relative_to(project_root))]


def run_reset_migration_stage(project_root: Path, stage: str, fqns: list[str]) -> ResetMigrationOutput:
    if stage not in RESETTABLE_STAGES:
        raise ValueError(f"Unsupported reset stage: {stage}")
    if not fqns:
        raise ValueError("reset-migration requires at least one FQN for staged resets")

    normalized = [normalize(fqn) for fqn in fqns]
    targets: list[ResetTargetResult] = []
    blocked: list[str] = []
    not_found: list[str] = []

    resolved_tables: list[tuple[str, dict[str, Any]]] = []
    for norm in normalized:
        bucket = detect_catalog_bucket(project_root, norm)
        if bucket != "tables":
            targets.append(
                ResetTargetResult(
                    fqn=norm,
                    status="not_found",
                    reason="table_catalog_not_found",
                )
            )
            not_found.append(norm)
            continue

        table_path = project_root / "catalog" / "tables" / f"{norm}.json"
        table_data = read_catalog_json(table_path)
        generate_status = (
            table_data.get("generate", {}).get("status")
            if isinstance(table_data.get("generate"), dict)
            else None
        )
        if generate_status == "ok":
            targets.append(
                ResetTargetResult(
                    fqn=norm,
                    status="blocked",
                    reason="generate_already_complete",
                )
            )
            blocked.append(norm)
            continue

        resolved_tables.append((norm, table_data))

    if blocked:
        return ResetMigrationOutput(
            stage=stage,
            targets=targets,
            reset=[],
            noop=[],
            blocked=blocked,
            not_found=not_found,
        )

    reset: list[str] = []
    noop: list[str] = []

    for norm, _table_data in resolved_tables:
        cleared_sections, deleted_files, mutated_files, writer = reset_table_sections(project_root, norm, stage)
        if stage in ("scope", "profile", "generate-tests", "refactor"):
            writer_sections, writer_files = reset_writer_refactor(project_root, writer)
            cleared_sections.extend(writer_sections)
            mutated_files.extend(writer_files)

        status = "reset" if cleared_sections or deleted_files else "noop"
        if status == "reset":
            reset.append(norm)
        else:
            noop.append(norm)

        targets.append(
            ResetTargetResult(
                fqn=norm,
                status=status,
                cleared_sections=cleared_sections,
                deleted_files=deleted_files,
                mutated_files=mutated_files,
            )
        )

    return ResetMigrationOutput(
        stage=stage,
        targets=targets,
        reset=reset,
        noop=noop,
        blocked=[],
        not_found=not_found,
    )


__all__ = ["reset_table_sections", "reset_writer_refactor", "run_reset_migration_stage"]
