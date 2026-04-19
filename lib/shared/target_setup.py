"""Shared target-setup orchestration helpers."""

from __future__ import annotations

from pathlib import Path

from shared.generate_sources import write_sources_yml
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.output_models.target_setup import SetupTargetOutput
from shared.target_setup_support.dbt_commands import (
    DbtCommandResult as DbtCommandResult,
    run_dbt_validation_command,
)
from shared.target_setup_support.dbt_scaffold import scaffold_target_project
from shared.target_setup_support.runtime import (
    ensure_setup_target_can_rerun,
    get_target_source_schema,
    write_target_runtime_from_env as write_target_runtime_from_env,
)
from shared.target_setup_support.seeds import (
    DbtSeedResult as DbtSeedResult,
    SeedColumnSpec as SeedColumnSpec,
    SeedExportResult as SeedExportResult,
    SeedTableSpec as SeedTableSpec,
    export_seed_tables,
    materialize_seed_tables,
)
from shared.target_setup_support.source_tables import (
    TargetApplyResult as TargetApplyResult,
    TargetTableSpec as TargetTableSpec,
    apply_target_source_tables,
    load_target_source_table_specs as load_target_source_table_specs,
)

__all__ = [
    "DbtCommandResult",
    "DbtSeedResult",
    "GenerateSourcesOutput",
    "SeedColumnSpec",
    "SeedExportResult",
    "SeedTableSpec",
    "SetupTargetOutput",
    "TargetApplyResult",
    "TargetTableSpec",
    "apply_target_source_tables",
    "ensure_setup_target_can_rerun",
    "export_seed_tables",
    "get_target_source_schema",
    "load_target_source_table_specs",
    "materialize_seed_tables",
    "run_dbt_validation_command",
    "run_setup_target",
    "scaffold_target_project",
    "write_sources_yml",
    "write_target_runtime_from_env",
    "write_target_sources_yml",
]


def write_target_sources_yml(project_root: Path) -> GenerateSourcesOutput:
    """Write sources.yml using the configured target source schema mapping."""
    return write_sources_yml(
        project_root,
        source_schema_override=get_target_source_schema(project_root),
        require_staging_contract_types=True,
    )


def run_setup_target(project_root: Path) -> SetupTargetOutput:
    """Execute the reusable target-setup orchestration for tests and callers."""
    ensure_setup_target_can_rerun(project_root)
    files = scaffold_target_project(project_root)
    sources = write_target_sources_yml(project_root)
    source_error = getattr(sources, "error", None)
    if isinstance(source_error, str) and source_error:
        message = getattr(sources, "message", None) or source_error
        raise ValueError(message)
    seeds = export_seed_tables(project_root)
    seed_materialization = materialize_seed_tables(project_root, seeds.csv_files)
    applied = apply_target_source_tables(project_root)
    generated_models = list(getattr(sources, "generated_model_names", []) or [])
    generated_source_selectors = list(getattr(sources, "generated_source_selectors", []) or [])
    dbt_compile = run_dbt_validation_command(project_root, "compile", generated_models)
    dbt_build = run_dbt_validation_command(
        project_root,
        "build",
        [*generated_models, *generated_source_selectors],
    )
    source_files = sources.written_paths if isinstance(sources.written_paths, list) else []
    if not source_files and sources.path:
        source_path = Path(sources.path)
        if source_path.is_absolute():
            try:
                source_files = [str(source_path.resolve().relative_to(project_root.resolve()))]
            except ValueError:
                source_files = [str(source_path)]
        else:
            source_files = [str(source_path)]
    written_paths = [*files, *source_files, *seeds.written_paths]
    return SetupTargetOutput(
        files=files + source_files + seeds.files,
        written_paths=written_paths,
        sources_path=sources.path,
        target_source_schema=applied.physical_schema,
        created_tables=applied.created_tables,
        existing_tables=applied.existing_tables,
        desired_tables=applied.desired_tables,
        seed_files=seeds.csv_files,
        seed_row_counts=seeds.row_counts,
        dbt_seed_ran=seed_materialization.ran,
        dbt_seed_command=seed_materialization.command,
        dbt_compile_ran=dbt_compile.ran,
        dbt_compile_command=dbt_compile.command,
        dbt_build_ran=dbt_build.ran,
        dbt_build_command=dbt_build.command,
    )
