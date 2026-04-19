"""Technology-specific source configuration for project initialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from shared.init_templates import (
    _claude_md_oracle,
    _claude_md_sql_server,
    _envrc_oracle,
    _envrc_sql_server,
    _pre_commit_hook_oracle,
    _pre_commit_hook_sql_server,
    _readme_md_oracle,
    _readme_md_sql_server,
    _repo_map_oracle,
    _repo_map_sql_server,
)


@dataclass(frozen=True)
class SourceConfig:
    """Technology-specific configuration for project scaffolding."""

    slug: str
    display_name: str
    env_vars: list[str]
    dep_group: str
    claude_md_fn: Callable[[], str]
    readme_md_fn: Callable[[], str]
    envrc_fn: Callable[[], str]
    repo_map_fn: Callable[[], dict[str, Any]]
    pre_commit_hook_fn: Callable[[], str]


SOURCE_REGISTRY: dict[str, SourceConfig] = {
    "sql_server": SourceConfig(
        slug="sql_server",
        display_name="SQL Server",
        env_vars=["SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PORT", "SOURCE_MSSQL_DB", "SOURCE_MSSQL_USER", "SOURCE_MSSQL_PASSWORD"],
        dep_group="export",
        claude_md_fn=_claude_md_sql_server,
        readme_md_fn=_readme_md_sql_server,
        envrc_fn=_envrc_sql_server,
        repo_map_fn=_repo_map_sql_server,
        pre_commit_hook_fn=_pre_commit_hook_sql_server,
    ),
    "oracle": SourceConfig(
        slug="oracle",
        display_name="Oracle",
        env_vars=["SOURCE_ORACLE_HOST", "SOURCE_ORACLE_PORT", "SOURCE_ORACLE_SERVICE", "SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD"],
        dep_group="oracle",
        claude_md_fn=_claude_md_oracle,
        readme_md_fn=_readme_md_oracle,
        envrc_fn=_envrc_oracle,
        repo_map_fn=_repo_map_oracle,
        pre_commit_hook_fn=_pre_commit_hook_oracle,
    ),
}


def get_source_config(technology: str) -> SourceConfig:
    """Look up a source config by slug. Raises ValueError for unknown slugs."""
    if technology not in SOURCE_REGISTRY:
        raise ValueError(
            f"Unknown technology: {technology!r}. "
            f"Must be one of {sorted(SOURCE_REGISTRY.keys())}."
        )
    return SOURCE_REGISTRY[technology]
