"""SQL dialect abstraction.

Defines the SqlDialect Protocol and a registry keyed by string name.
Skills request a dialect via get_dialect("tsql") rather than importing
a concrete class directly, keeping skill code dialect-agnostic.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import sqlglot


@runtime_checkable
class SqlDialect(Protocol):
    """Minimal protocol every dialect implementation must satisfy."""

    @property
    def name(self) -> str:
        """Canonical lowercase dialect name (e.g. 'tsql', 'spark')."""
        ...

    def parse(self, sql: str) -> Any:
        """Parse a SQL string and return a sqlglot expression tree."""
        ...

    def transpile_to(self, sql: str, target: "SqlDialect") -> str:
        """Transpile sql from this dialect to the target dialect."""
        ...


class TsqlDialect:
    """T-SQL (Microsoft SQL Server / Fabric Warehouse) dialect."""

    @property
    def name(self) -> str:
        return "tsql"

    def parse(self, sql: str) -> Any:
        return sqlglot.parse_one(sql, dialect="tsql", error_level=sqlglot.ErrorLevel.WARN)

    def transpile_to(self, sql: str, target: SqlDialect) -> str:
        results = sqlglot.transpile(sql, read="tsql", write=target.name, error_level=sqlglot.ErrorLevel.WARN)
        return ";\n".join(results)


class SparkDialect:
    """Spark SQL (Fabric Lakehouse target) dialect."""

    @property
    def name(self) -> str:
        return "spark"

    def parse(self, sql: str) -> Any:
        return sqlglot.parse_one(sql, dialect="spark", error_level=sqlglot.ErrorLevel.WARN)

    def transpile_to(self, sql: str, target: SqlDialect) -> str:
        results = sqlglot.transpile(sql, read="spark", write=target.name, error_level=sqlglot.ErrorLevel.WARN)
        return ";\n".join(results)


_REGISTRY: dict[str, SqlDialect] = {
    "tsql": TsqlDialect(),
    "spark": SparkDialect(),
}


def get_dialect(name: str) -> SqlDialect:
    """Return the registered dialect for the given name.

    Raises:
        KeyError: if the dialect name is not registered.
    """
    key = name.lower()
    if key not in _REGISTRY:
        raise KeyError(f"Unknown dialect '{name}'. Registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]


def register_dialect(dialect: SqlDialect) -> None:
    """Register a custom dialect implementation."""
    _REGISTRY[dialect.name] = dialect
