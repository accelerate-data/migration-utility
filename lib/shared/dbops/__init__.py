"""Technology-specific DB operation adapters."""

from shared.dbops.base import ColumnSpec, DatabaseOperations
from shared.dbops.oracle import OracleOperations
from shared.dbops.sql_server import SqlServerOperations

_REGISTRY = {
    "oracle": OracleOperations,
    "sql_server": SqlServerOperations,
}


def get_dbops(technology: str) -> type[DatabaseOperations]:
    try:
        return _REGISTRY[technology]
    except KeyError as exc:
        raise ValueError(f"Unsupported DB operations technology: {technology}") from exc


__all__ = [
    "DatabaseOperations",
    "ColumnSpec",
    "OracleOperations",
    "SqlServerOperations",
    "get_dbops",
]
