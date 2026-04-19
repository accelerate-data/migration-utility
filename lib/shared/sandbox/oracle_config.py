"""Oracle sandbox runtime configuration loading."""

from __future__ import annotations

import os
from typing import Any

from shared.runtime_config import get_runtime_role


class OracleSandboxConfigMixin:
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

        if source_role.technology != "oracle":
            raise ValueError("runtime.source.technology must be oracle for Oracle sandbox")
        if sandbox_role.technology != "oracle":
            raise ValueError("runtime.sandbox.technology must be oracle for Oracle sandbox")

        host = sandbox_role.connection.host or ""
        port = sandbox_role.connection.port or "1521"
        cdb_service = sandbox_role.connection.service or ""
        admin_user = sandbox_role.connection.user or ""
        password_env = sandbox_role.connection.password_env
        password = os.environ.get(password_env or "", "")
        source_host = source_role.connection.host or ""
        source_port = source_role.connection.port or "1521"
        source_service = source_role.connection.service or ""
        source_user = source_role.connection.user or ""
        source_password_env = source_role.connection.password_env
        source_password = os.environ.get(source_password_env or "", "")
        source_schema = source_role.connection.schema_name or ""

        if not host:
            missing.append("runtime.sandbox.connection.host")
        if not sandbox_role.connection.port:
            missing.append("runtime.sandbox.connection.port")
        if not cdb_service:
            missing.append("runtime.sandbox.connection.service")
        if not admin_user:
            missing.append("runtime.sandbox.connection.user")
        if not password_env:
            missing.append("runtime.sandbox.connection.password_env")
        if not password:
            missing.append(
                "environment variable referenced by runtime.sandbox.connection.password_env "
                f"({password_env})"
            )
        if not source_host:
            missing.append("runtime.source.connection.host")
        if not source_role.connection.port:
            missing.append("runtime.source.connection.port")
        if not source_service:
            missing.append("runtime.source.connection.service")
        if not source_user:
            missing.append("runtime.source.connection.user")
        if not source_password_env:
            missing.append("runtime.source.connection.password_env")
        if not source_password:
            missing.append(
                "environment variable referenced by runtime.source.connection.password_env "
                f"({source_password_env})"
            )
        if not source_schema:
            missing.append("runtime.source.connection.schema")
        if missing:
            raise ValueError(f"Required sandbox configuration is missing: {missing}")

        return cls(
            host=host,
            port=port,
            cdb_service=cdb_service,
            password=password,
            admin_user=admin_user,
            source_schema=source_schema,
            source_host=source_host,
            source_port=source_port,
            source_service=source_service,
            source_user=source_user,
            source_password=source_password,
        )
