"""Shared target-setup orchestration helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.dbops import ColumnSpec, get_dbops
from shared.generate_sources import generate_sources, write_sources_yml
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.runtime_config import get_runtime_role
from shared.runtime_config_models import RuntimeRole


@dataclass(frozen=True)
class TargetTableSpec:
    """One source-backed table that should exist on the target."""

    logical_schema: str
    physical_schema: str
    table_name: str
    columns: list[ColumnSpec]

    @property
    def fqn(self) -> str:
        return f"{self.physical_schema}.{self.table_name}"


@dataclass(frozen=True)
class TargetApplyResult:
    """Outcome of applying source-backed target tables."""

    physical_schema: str
    desired_tables: list[str]
    created_tables: list[str]
    existing_tables: list[str]


def read_manifest(project_root: Path) -> dict:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError("manifest.json not found. Run /setup-ddl first.")
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"manifest.json is not valid JSON: {exc}") from exc


def _require_target_role(project_root: Path) -> RuntimeRole:
    manifest = read_manifest(project_root)
    target_role = get_runtime_role(manifest, "target")
    if target_role is None:
        raise ValueError("manifest.json is missing runtime.target. Run /setup-target first.")
    return target_role


def get_target_source_schema(project_root: Path, default: str = "bronze") -> str:
    """Return the configured target source schema, defaulting to bronze."""
    target_role = _require_target_role(project_root)
    if target_role.schemas is None or not target_role.schemas.source:
        return default
    return target_role.schemas.source


def _dbt_profile_name(project_root: Path) -> str:
    return project_root.name.replace("-", "_") or "migration_target"


def _dbt_adapter_type(technology: str) -> str:
    return {
        "sql_server": "sqlserver",
        "oracle": "oracle",
        "duckdb": "duckdb",
    }[technology]


def _render_profiles_yml(profile_name: str, target_role: RuntimeRole, target_source_schema: str) -> str:
    technology = target_role.technology
    adapter_type = _dbt_adapter_type(technology)
    connection = target_role.connection
    if technology == "sql_server":
        password_env = connection.password_env
        if not password_env:
            raise ValueError("runtime.target.connection.password_env is required for SQL Server target setup")
        driver = connection.driver or "ODBC Driver 18 for SQL Server"
        user = connection.user or "sa"
        host = connection.host or "localhost"
        port = connection.port or "1433"
        database = connection.database or "MigrationTarget"
        return (
            f"{profile_name}:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            f"      type: {adapter_type}\n"
            f"      driver: \"{driver}\"\n"
            f"      server: \"{host}\"\n"
            f"      port: {int(port)}\n"
            f"      database: \"{database}\"\n"
            f"      user: \"{user}\"\n"
            f"      password: \"{{{{ env_var('{password_env}') }}}}\"\n"
            f"      schema: \"{target_source_schema}\"\n"
            "      trust_cert: true\n"
            "      threads: 4\n"
        )
    if technology == "oracle":
        password_env = connection.password_env
        if not password_env:
            raise ValueError("runtime.target.connection.password_env is required for Oracle target setup")
        user = connection.user or "system"
        host = connection.host or "localhost"
        port = connection.port or "1521"
        service = connection.service or "FREEPDB1"
        schema = connection.schema_name or user
        return (
            f"{profile_name}:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            f"      type: {adapter_type}\n"
            f"      host: \"{host}\"\n"
            f"      port: {int(port)}\n"
            f"      service: \"{service}\"\n"
            f"      user: \"{user}\"\n"
            f"      password: \"{{{{ env_var('{password_env}') }}}}\"\n"
            f"      schema: \"{schema}\"\n"
            "      threads: 4\n"
        )
    if technology == "duckdb":
        path = connection.path or ".runtime/duckdb/target.duckdb"
        return (
            f"{profile_name}:\n"
            "  target: dev\n"
            "  outputs:\n"
            "    dev:\n"
            f"      type: {adapter_type}\n"
            f"      path: \"{path}\"\n"
            f"      schema: \"{target_source_schema}\"\n"
            "      threads: 4\n"
        )
    raise ValueError(f"Unsupported target technology: {technology}")


def _render_dbt_project_yml(profile_name: str) -> str:
    return (
        f"name: \"{profile_name}\"\n"
        "version: \"1.0.0\"\n"
        "config-version: 2\n\n"
        f"profile: \"{profile_name}\"\n\n"
        "model-paths: [\"models\"]\n"
        "snapshot-paths: [\"snapshots\"]\n"
        "macro-paths: [\"macros\"]\n"
        "test-paths: [\"tests\"]\n"
        "target-path: \"target\"\n"
        "clean-targets: [\"target\"]\n"
    )


def scaffold_target_project(project_root: Path) -> list[str]:
    """Create or preserve the minimal dbt scaffold needed by setup-target."""
    target_role = _require_target_role(project_root)
    profile_name = _dbt_profile_name(project_root)
    target_source_schema = get_target_source_schema(project_root)
    dbt_root = project_root / "dbt"
    created_or_updated: list[str] = []

    for relative_dir in ("models/staging", "macros", "snapshots", "tests"):
        (dbt_root / relative_dir).mkdir(parents=True, exist_ok=True)

    dbt_project_path = dbt_root / "dbt_project.yml"
    desired_project = _render_dbt_project_yml(profile_name)
    if not dbt_project_path.exists() or dbt_project_path.read_text(encoding="utf-8") != desired_project:
        dbt_project_path.write_text(desired_project, encoding="utf-8")
        created_or_updated.append(str(dbt_project_path.relative_to(project_root)))

    profiles_path = dbt_root / "profiles.yml"
    desired_profiles = _render_profiles_yml(profile_name, target_role, target_source_schema)
    if not profiles_path.exists() or profiles_path.read_text(encoding="utf-8") != desired_profiles:
        profiles_path.write_text(desired_profiles, encoding="utf-8")
        created_or_updated.append(str(profiles_path.relative_to(project_root)))

    return created_or_updated


def _load_source_table_specs(project_root: Path) -> list[TargetTableSpec]:
    target_schema = get_target_source_schema(project_root)
    tables_dir = project_root / "catalog" / "tables"
    if not tables_dir.is_dir():
        return []

    specs: list[TargetTableSpec] = []
    for table_file in sorted(tables_dir.glob("*.json")):
        payload = json.loads(table_file.read_text(encoding="utf-8"))
        if payload.get("excluded") or payload.get("is_source") is not True:
            continue
        logical_schema = str(payload.get("schema", "")).lower()
        table_name = str(payload.get("name", ""))
        if not logical_schema or not table_name:
            continue
        columns = []
        for column in payload.get("columns", []):
            source_type = (
                column.get("sql_type")
                or column.get("data_type")
                or column.get("type")
                or "VARCHAR"
            )
            columns.append(
                ColumnSpec(
                    name=column["name"],
                    source_type=str(source_type),
                    nullable=bool(column.get("is_nullable", True)),
                )
            )
        if not columns:
            columns.append(ColumnSpec(name="id", source_type="BIGINT", nullable=False))
        specs.append(
            TargetTableSpec(
                logical_schema=logical_schema,
                physical_schema=target_schema,
                table_name=table_name,
                columns=columns,
            )
        )
    return specs


def generate_target_sources(project_root: Path) -> GenerateSourcesOutput:
    """Build logical dbt sources remapped to the configured target source schema."""
    return generate_sources(
        project_root,
        source_schema_override=get_target_source_schema(project_root),
    )


def write_target_sources_yml(project_root: Path) -> GenerateSourcesOutput:
    """Write sources.yml using the configured target source schema mapping."""
    return write_sources_yml(
        project_root,
        source_schema_override=get_target_source_schema(project_root),
    )


def apply_target_source_tables(project_root: Path) -> TargetApplyResult:
    """Ensure confirmed source tables exist on the configured target schema."""
    target_role = _require_target_role(project_root)
    target_schema = get_target_source_schema(project_root)
    adapter = get_dbops(target_role.technology).from_role(
        target_role,
        project_root=project_root,
    )
    desired_specs = _load_source_table_specs(project_root)

    adapter.ensure_source_schema(target_schema)
    existing = adapter.list_source_tables(target_schema)

    created_tables: list[str] = []
    existing_tables: list[str] = []
    for spec in desired_specs:
        if spec.table_name.lower() in existing:
            existing_tables.append(spec.fqn)
            continue
        adapter.create_source_table(spec.physical_schema, spec.table_name, spec.columns)
        created_tables.append(spec.fqn)

    return TargetApplyResult(
        physical_schema=target_schema,
        desired_tables=[spec.fqn for spec in desired_specs],
        created_tables=created_tables,
        existing_tables=existing_tables,
    )


def run_setup_target(project_root: Path) -> dict[str, Any]:
    """Execute the reusable target-setup orchestration for tests and callers."""
    files = scaffold_target_project(project_root)
    sources = write_target_sources_yml(project_root)
    applied = apply_target_source_tables(project_root)
    return {
        "files": files,
        "sources_path": sources.path,
        "target_source_schema": applied.physical_schema,
        "created_tables": applied.created_tables,
        "existing_tables": applied.existing_tables,
        "desired_tables": applied.desired_tables,
    }
