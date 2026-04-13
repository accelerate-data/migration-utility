"""Technology-specific sandbox backends for the test harness.

Each backend implements the sandbox lifecycle: create a throwaway database,
clone schema from the source, execute test scenarios, and tear down.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.sandbox.base import SandboxBackend


_BACKENDS: dict[str, str] = {
    "sql_server": "shared.sandbox.sql_server:SqlServerSandbox",
    "oracle": "shared.sandbox.oracle:OracleSandbox",
    "duckdb": "shared.sandbox.duckdb:DuckDbSandbox",
}


def get_backend(technology: str) -> type[SandboxBackend]:
    """Return the backend class for the given technology.

    Uses lazy imports so pyodbc is only required when actually used.
    """
    dotted = _BACKENDS.get(technology)
    if dotted is None:
        supported = sorted(_BACKENDS)
        raise ValueError(
            f"Unsupported technology: {technology!r}. "
            f"Supported: {supported}"
        )
    module_path, class_name = dotted.rsplit(":", 1)
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)  # type: ignore[no-any-return]
