"""SQL Server sandbox runtime configuration loading."""

from __future__ import annotations

import os
from typing import Any

from shared.db_connect import SQL_SERVER_ODBC_DRIVER
from shared.runtime_config import get_runtime_role


class SqlServerSandboxConfigMixin:
    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> Any:
        """Create an instance from strict runtime roles plus process secrets."""
        source_role = get_runtime_role(manifest, "source")
        sandbox_role = get_runtime_role(manifest, "sandbox")

        missing: list[str] = []
        if source_role is None:
            missing.append("runtime.source")
        if sandbox_role is None:
            missing.append("runtime.sandbox")
        if missing:
            raise ValueError(f"manifest.json is missing required runtime roles: {missing}")

        if source_role.technology != "sql_server":
            raise ValueError("runtime.source.technology must be sql_server for SQL Server sandbox")
        if sandbox_role.technology != "sql_server":
            raise ValueError("runtime.sandbox.technology must be sql_server for SQL Server sandbox")

        sandbox_host = sandbox_role.connection.host or ""
        sandbox_port = sandbox_role.connection.port or "1433"
        sandbox_user = sandbox_role.connection.user or ""
        sandbox_driver = SQL_SERVER_ODBC_DRIVER
        sandbox_password_env = sandbox_role.connection.password_env
        sandbox_password = os.environ.get(sandbox_password_env or "", "")

        source_host = source_role.connection.host or ""
        source_port = source_role.connection.port or "1433"
        source_database = source_role.connection.database or ""
        source_user = source_role.connection.user or "sa"
        source_driver = SQL_SERVER_ODBC_DRIVER
        source_password_env = source_role.connection.password_env
        source_password = os.environ.get(source_password_env or "", "")

        if not sandbox_host:
            missing.append("runtime.sandbox.connection.host")
        if not sandbox_role.connection.port:
            missing.append("runtime.sandbox.connection.port")
        if not sandbox_user:
            missing.append("runtime.sandbox.connection.user")
        if not sandbox_password_env:
            missing.append("runtime.sandbox.connection.password_env")
        if not sandbox_password:
            missing.append(
                "environment variable referenced by runtime.sandbox.connection.password_env "
                f"({sandbox_password_env})"
            )
        if not source_host:
            missing.append("runtime.source.connection.host")
        if not source_role.connection.port:
            missing.append("runtime.source.connection.port")
        if not source_database:
            missing.append("runtime.source.connection.database")
        if not source_password_env:
            missing.append("runtime.source.connection.password_env")
        if not source_password:
            missing.append(
                "environment variable referenced by runtime.source.connection.password_env "
                f"({source_password_env})"
            )
        if missing:
            raise ValueError(f"Required sandbox configuration is missing: {missing}")

        return cls(
            host=sandbox_host,
            port=sandbox_port,
            password=sandbox_password,
            user=sandbox_user,
            driver=sandbox_driver,
            source_host=source_host,
            source_port=source_port,
            source_database=source_database,
            source_user=source_user,
            source_password=source_password,
            source_driver=source_driver,
        )
