"""dbt project and profile scaffold rendering for target setup."""

from __future__ import annotations

from pathlib import Path

from shared.db_connect import SQL_SERVER_ODBC_DRIVER
from shared.runtime_config_models import RuntimeRole
from shared.target_setup_support.runtime import get_target_source_schema, require_target_role


def dbt_profile_name(project_root: Path) -> str:
    return project_root.name.replace("-", "_") or "migration_target"


def dbt_adapter_type(technology: str) -> str:
    adapters = {
        "sql_server": "sqlserver",
        "oracle": "oracle",
    }
    if technology not in adapters:
        raise ValueError(f"Unknown technology: {technology}")
    return adapters[technology]


def render_profiles_yml(profile_name: str, target_role: RuntimeRole, target_source_schema: str) -> str:
    technology = target_role.technology
    adapter_type = dbt_adapter_type(technology)
    connection = target_role.connection
    if technology == "sql_server":
        password_env = connection.password_env
        if not password_env:
            raise ValueError("runtime.target.connection.password_env is required for SQL Server target setup")
        driver = SQL_SERVER_ODBC_DRIVER
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
            "      protocol: \"tcp\"\n"
            f"      database: \"{service}\"\n"
            f"      host: \"{host}\"\n"
            f"      port: {int(port)}\n"
            f"      service: \"{service}\"\n"
            f"      user: \"{user}\"\n"
            f"      password: \"{{{{ env_var('{password_env}') }}}}\"\n"
            f"      schema: \"{target_source_schema}\"\n"
            "      threads: 4\n"
        )
    raise ValueError(f"Unsupported target technology: {technology}")


def render_dbt_project_yml(profile_name: str) -> str:
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
        "\n"
        "models:\n"
        f"  {profile_name}:\n"
        "    staging:\n"
        "      +materialized: view\n"
        "    intermediate:\n"
        "      +materialized: ephemeral\n"
        "    marts:\n"
        "      +materialized: table\n"
    )


def scaffold_target_project(project_root: Path) -> list[str]:
    """Create or preserve the minimal dbt scaffold needed by setup-target."""
    target_role = require_target_role(project_root)
    profile_name = dbt_profile_name(project_root)
    target_source_schema = get_target_source_schema(project_root)
    dbt_root = project_root / "dbt"
    created_or_updated: list[str] = []

    for relative_dir in (
        "models/staging",
        "models/intermediate",
        "models/marts",
        "macros",
        "seeds",
        "snapshots",
        "tests",
    ):
        (dbt_root / relative_dir).mkdir(parents=True, exist_ok=True)

    dbt_project_path = dbt_root / "dbt_project.yml"
    desired_project = render_dbt_project_yml(profile_name)
    if not dbt_project_path.exists() or dbt_project_path.read_text(encoding="utf-8") != desired_project:
        dbt_project_path.write_text(desired_project, encoding="utf-8")
        created_or_updated.append(str(dbt_project_path.relative_to(project_root)))

    profiles_path = dbt_root / "profiles.yml"
    desired_profiles = render_profiles_yml(profile_name, target_role, target_source_schema)
    if not profiles_path.exists() or profiles_path.read_text(encoding="utf-8") != desired_profiles:
        profiles_path.write_text(desired_profiles, encoding="utf-8")
        created_or_updated.append(str(profiles_path.relative_to(project_root)))

    return created_or_updated
