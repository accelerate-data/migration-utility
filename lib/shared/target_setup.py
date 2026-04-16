"""Shared target-setup orchestration helpers."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from csv import writer as csv_writer
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from shared.dbops import ColumnSpec, get_dbops
from shared.generate_sources import generate_sources, write_sources_yml
from shared.name_resolver import model_name_from_table, normalize
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.output_models.target_setup import SetupTargetOutput
from shared.runtime_config import dialect_for_technology, get_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole, RuntimeSchemas
from shared.setup_ddl_support.manifest import read_manifest_strict

logger = logging.getLogger(__name__)

_TARGET_ENV_MAPS: dict[str, dict[str, str]] = {
    "sql_server": {
        "host": "TARGET_MSSQL_HOST",
        "port": "TARGET_MSSQL_PORT",
        "database": "TARGET_MSSQL_DB",
        "user": "TARGET_MSSQL_USER",
        "password_env": "TARGET_MSSQL_PASSWORD",
    },
    "oracle": {
        "host": "TARGET_ORACLE_HOST",
        "port": "TARGET_ORACLE_PORT",
        "service": "TARGET_ORACLE_SERVICE",
        "user": "TARGET_ORACLE_USER",
        "password_env": "TARGET_ORACLE_PASSWORD",
    },
}


def write_target_runtime_from_env(
    project_root: Path,
    technology: str,
    source_schema: str = "bronze",
) -> RuntimeRole:
    """Read TARGET_* env vars and write runtime.target to manifest.json.

    Returns the RuntimeRole written. Raises ValueError if manifest is missing.
    """
    if technology not in _TARGET_ENV_MAPS:
        raise ValueError(
            f"Unknown target technology '{technology}'. "
            f"Supported: {list(_TARGET_ENV_MAPS)}"
        )

    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"manifest.json not found at {manifest_path}. Run setup-source first.")

    env_map = _TARGET_ENV_MAPS[technology]
    connection_kwargs: dict[str, str] = {}
    for field, env_var in env_map.items():
        if field == "password_env":
            # Store the env var name itself — not the secret value.
            connection_kwargs["password_env"] = env_var
        else:
            value = os.environ.get(env_var, "")
            if value:
                connection_kwargs[field] = value

    role = RuntimeRole(
        technology=technology,
        dialect=dialect_for_technology(technology),
        connection=RuntimeConnection(**connection_kwargs),
        schemas=RuntimeSchemas(source=source_schema, marts=None),
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if "runtime" not in manifest or not isinstance(manifest["runtime"], dict):
        manifest["runtime"] = {}
    manifest["runtime"]["target"] = role.model_dump(mode="json", exclude_none=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    logger.info(
        "event=write_target_runtime status=success component=target_setup technology=%s source_schema=%s",
        technology,
        source_schema,
    )

    return role


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


@dataclass(frozen=True)
class SeedTableSpec:
    """One catalog seed table that should be exported as a dbt seed CSV."""

    logical_schema: str
    table_name: str
    columns: list[str]

    @property
    def fqn(self) -> str:
        return f"{self.logical_schema}.{self.table_name.lower()}"

    @property
    def seed_name(self) -> str:
        return model_name_from_table(self.fqn)


@dataclass(frozen=True)
class SeedExportResult:
    """Outcome of exporting dbt seed CSV files from source tables."""

    files: list[str]
    row_counts: dict[str, int]


@dataclass(frozen=True)
class DbtSeedResult:
    """Outcome of invoking dbt seed for exported seed CSV files."""

    ran: bool
    command: list[str]


def _require_target_role(project_root: Path) -> RuntimeRole:
    manifest = read_manifest_strict(project_root)
    target_role = get_runtime_role(manifest, "target")
    if target_role is None:
        raise ValueError("manifest.json is missing runtime.target. Run /setup-target first.")
    return target_role


def _require_source_role(project_root: Path) -> RuntimeRole:
    manifest = read_manifest_strict(project_root)
    source_role = get_runtime_role(manifest, "source")
    if source_role is None:
        raise ValueError("manifest.json is missing runtime.source. Run /setup-source first.")
    return source_role


def get_target_source_schema(project_root: Path, default: str = "bronze") -> str:
    """Return the configured target source schema, defaulting to bronze."""
    target_role = _require_target_role(project_root)
    if target_role.schemas is None or not target_role.schemas.source:
        return default
    return target_role.schemas.source


def _dbt_profile_name(project_root: Path) -> str:
    return project_root.name.replace("-", "_") or "migration_target"


def _dbt_adapter_type(technology: str) -> str:
    adapters = {
        "sql_server": "sqlserver",
        "oracle": "oracle",
    }
    if technology not in adapters:
        raise ValueError(f"Unknown technology: {technology}")
    return adapters[technology]


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
        "seed-paths: [\"seeds\"]\n"
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

    for relative_dir in ("models/staging", "macros", "seeds", "snapshots", "tests"):
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


def _load_seed_table_specs(project_root: Path) -> list[SeedTableSpec]:
    tables_dir = project_root / "catalog" / "tables"
    if not tables_dir.is_dir():
        return []

    specs: list[SeedTableSpec] = []
    seen_seed_names: dict[str, str] = {}
    for table_file in sorted(tables_dir.glob("*.json")):
        payload = json.loads(table_file.read_text(encoding="utf-8"))
        if payload.get("excluded") or payload.get("is_seed") is not True:
            continue
        logical_schema = str(payload.get("schema", "")).lower()
        table_name = str(payload.get("name", ""))
        if not logical_schema or not table_name:
            continue
        fqn = normalize(f"{logical_schema}.{table_name}")
        seed_name = model_name_from_table(fqn)
        if existing_fqn := seen_seed_names.get(seed_name):
            raise ValueError(
                f"Seed table {fqn!r} maps to dbt seed name {seed_name!r}, "
                f"which is already used by {existing_fqn!r}."
            )
        seen_seed_names[seed_name] = fqn
        columns = [
            str(column["name"])
            for column in payload.get("columns", [])
            if isinstance(column, dict) and column.get("name")
        ]
        specs.append(
            SeedTableSpec(
                logical_schema=logical_schema,
                table_name=table_name,
                columns=columns,
            )
        )
    return specs


def _render_seed_csv(columns: list[str], rows: list[tuple[object, ...]]) -> str:
    buffer = StringIO()
    writer = csv_writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()


def export_seed_tables(project_root: Path) -> SeedExportResult:
    """Export confirmed seed catalog tables from source DB into dbt seed CSVs."""
    seed_specs = _load_seed_table_specs(project_root)
    if not seed_specs:
        return SeedExportResult(files=[], row_counts={})

    source_role = _require_source_role(project_root)
    adapter = get_dbops(source_role.technology).from_role(
        source_role,
        project_root=project_root,
    )
    dbt_root = project_root / "dbt"
    seeds_dir = dbt_root / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)

    files: list[str] = []
    row_counts: dict[str, int] = {}
    for spec in seed_specs:
        columns, rows = adapter.read_table_rows(spec.logical_schema, spec.table_name, spec.columns)
        seed_path = seeds_dir / f"{spec.seed_name}.csv"
        content = _render_seed_csv(columns, rows)
        if not seed_path.exists() or seed_path.read_text(encoding="utf-8") != content:
            seed_path.write_text(content, encoding="utf-8")
        relative_path = str(seed_path.relative_to(project_root))
        files.append(relative_path)
        row_counts[spec.fqn] = len(rows)
        logger.info(
            "event=export_seed_table component=target_setup table=%s seed_file=%s rows=%d status=success",
            spec.fqn,
            relative_path,
            len(rows),
        )

    return SeedExportResult(files=files, row_counts=row_counts)


def materialize_seed_tables(project_root: Path, seed_files: list[str]) -> DbtSeedResult:
    """Run dbt seed so exported seed CSVs are materialized in the target schema."""
    if not seed_files:
        return DbtSeedResult(ran=False, command=[])

    dbt_root = project_root / "dbt"
    command = [
        "dbt",
        "seed",
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=dbt_root,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ValueError("dbt executable not found on PATH; install dbt before running setup-target for seeds.") from exc
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()
        message = "dbt seed failed while materializing seed tables"
        if details:
            message = f"{message}: {details}"
        raise ValueError(message)

    logger.info(
        "event=dbt_seed_complete component=target_setup seed_files=%d status=success",
        len(seed_files),
    )
    return DbtSeedResult(ran=True, command=command)


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


def run_setup_target(project_root: Path) -> SetupTargetOutput:
    """Execute the reusable target-setup orchestration for tests and callers."""
    files = scaffold_target_project(project_root)
    sources = write_target_sources_yml(project_root)
    seeds = export_seed_tables(project_root)
    seed_materialization = materialize_seed_tables(project_root, seeds.files)
    applied = apply_target_source_tables(project_root)
    return SetupTargetOutput(
        files=files + seeds.files,
        sources_path=sources.path,
        target_source_schema=applied.physical_schema,
        created_tables=applied.created_tables,
        existing_tables=applied.existing_tables,
        desired_tables=applied.desired_tables,
        seed_files=seeds.files,
        seed_row_counts=seeds.row_counts,
        dbt_seed_ran=seed_materialization.ran,
        dbt_seed_command=seed_materialization.command,
    )
