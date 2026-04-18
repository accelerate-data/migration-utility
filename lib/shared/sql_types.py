"""SQL type formatting utilities.

Single source of truth for formatting column types from sys.columns (SQL Server)
and ALL_TAB_COLUMNS (Oracle) metadata.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SqlTypeSpec:
    """Structured SQL type before rendering catalog strings."""

    name: str
    length: int | str | None = None
    precision: int | None = None
    scale: int | None = None


class TypeMappingError(ValueError):
    """Raised when a source or target type mapping is unsupported."""


_INTEGER_TYPES = {"BIGINT", "INT", "SMALLINT", "TINYINT"}
_TEXT_TYPES = {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "VARCHAR2", "NVARCHAR2"}
_BINARY_TYPES = {"BINARY", "VARBINARY", "RAW"}


def format_sql_type(
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
) -> str:
    """Format a column type string from database metadata.

    Rules:
        NVARCHAR/VARCHAR/NCHAR/CHAR/BINARY/VARBINARY: TYPE(MAX) if -1, else length
            (N-prefixed T-SQL types are halved: stored as bytes, displayed as chars)
        VARCHAR2: Oracle char type, length from max_length as-is
        DECIMAL/NUMERIC/NUMBER: TYPE(precision,scale) if precision > 0, else bare type
        All others: bare type
    """
    tn = type_name.upper()
    if tn in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "BINARY", "VARBINARY"):
        if max_length == -1:
            length = "MAX"
        elif tn.startswith("N"):
            length = str(max_length // 2)
        else:
            length = str(max_length)
        return f"{tn}({length})"
    if tn == "VARCHAR2":
        return f"VARCHAR2({max_length})" if max_length > 0 else "VARCHAR2"
    if tn in ("DECIMAL", "NUMERIC"):
        return f"{tn}({precision},{scale})" if precision > 0 else tn
    if tn == "NUMBER":
        return f"NUMBER({precision},{scale})" if precision > 0 else "NUMBER"
    return tn


def _source_type_spec(
    source_technology: str,
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
) -> SqlTypeSpec:
    tn = type_name.upper()
    if tn in _TEXT_TYPES | _BINARY_TYPES:
        if max_length == -1:
            length: int | str | None = "MAX"
        elif source_technology == "sql_server" and tn in {"NVARCHAR", "NCHAR"}:
            length = max_length // 2
        elif max_length > 0:
            length = max_length
        else:
            length = None
        return SqlTypeSpec(tn, length=length)
    if tn in {"DECIMAL", "NUMERIC", "NUMBER"}:
        return SqlTypeSpec(tn, precision=precision or None, scale=scale)
    return SqlTypeSpec(tn)


def _canonical_from_source(source_technology: str, source: SqlTypeSpec) -> SqlTypeSpec:
    source_name = source.name.upper()
    if source_technology == "sql_server":
        if source_name in _INTEGER_TYPES:
            return SqlTypeSpec(source_name)
        if source_name in {"DECIMAL", "NUMERIC"}:
            return SqlTypeSpec("DECIMAL", precision=source.precision, scale=source.scale)
        if source_name in {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "BINARY", "VARBINARY"}:
            return source
        if source_name in {"BIT", "FLOAT", "REAL", "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIME"}:
            return SqlTypeSpec(source_name)
        if source_name == "DATETIMEOFFSET":
            return SqlTypeSpec("DATETIMEOFFSET")
        if source_name in {"MONEY", "SMALLMONEY"}:
            return SqlTypeSpec("DECIMAL", precision=19, scale=4)
        if source_name == "UNIQUEIDENTIFIER":
            return SqlTypeSpec("UNIQUEIDENTIFIER")
    elif source_technology == "oracle":
        if source_name == "NUMBER":
            if source.precision is None:
                return SqlTypeSpec("DECIMAL")
            if source.scale == 0:
                if source.precision <= 4:
                    return SqlTypeSpec("SMALLINT")
                if source.precision <= 9:
                    return SqlTypeSpec("INT")
                if source.precision <= 18:
                    return SqlTypeSpec("BIGINT")
            return SqlTypeSpec("DECIMAL", precision=source.precision, scale=source.scale)
        if source_name in {"VARCHAR2", "NVARCHAR2"}:
            return SqlTypeSpec("NVARCHAR" if source_name == "NVARCHAR2" else "VARCHAR", length=source.length)
        if source_name in {"CHAR", "NCHAR"}:
            return SqlTypeSpec(source_name, length=source.length)
        if source_name == "RAW":
            return SqlTypeSpec("VARBINARY", length=source.length)
        if source_name == "DATE":
            return SqlTypeSpec("DATETIME")
        if source_name.startswith("TIMESTAMP"):
            return SqlTypeSpec("DATETIME2")
        if source_name in {"BINARY_FLOAT", "BINARY_DOUBLE", "FLOAT"}:
            return SqlTypeSpec("FLOAT")
    else:
        raise TypeMappingError(f"Unsupported source technology: {source_technology}")

    raise TypeMappingError(f"Unsupported source type for {source_technology}: {source.name}")


def _render_source(source_technology: str, spec: SqlTypeSpec) -> str:
    if source_technology == "sql_server":
        return _render_tsql(spec)
    if source_technology == "oracle":
        name = spec.name.upper()
        if name in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"}:
            return f"{name}({spec.length})" if spec.length is not None else name
        if name == "NUMBER" and spec.precision is not None:
            return f"NUMBER({spec.precision},{spec.scale or 0})"
        return name
    raise TypeMappingError(f"Unsupported source technology: {source_technology}")


def _render_tsql(spec: SqlTypeSpec) -> str:
    name = spec.name.upper()
    if name in {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "BINARY", "VARBINARY"} and spec.length is not None:
        return f"{name}({spec.length})"
    if name == "DECIMAL" and spec.precision is not None:
        return f"DECIMAL({spec.precision},{spec.scale or 0})"
    return name


def _render_oracle(spec: SqlTypeSpec) -> str:
    name = spec.name.upper()
    if name in {"BIGINT", "INT", "SMALLINT", "TINYINT"}:
        precision = {"BIGINT": 19, "INT": 10, "SMALLINT": 5, "TINYINT": 3}[name]
        return f"NUMBER({precision},0)"
    if name == "DECIMAL":
        if spec.precision is None:
            return "NUMBER"
        return f"NUMBER({spec.precision},{spec.scale or 0})"
    if name in {"VARCHAR", "NVARCHAR", "CHAR", "NCHAR"}:
        if spec.length == "MAX":
            raise TypeMappingError(f"Unsupported Oracle target length for {spec.name}: MAX")
        oracle_name = {"VARCHAR": "VARCHAR2", "NVARCHAR": "NVARCHAR2"}.get(name, name)
        return f"{oracle_name}({spec.length})" if spec.length is not None else oracle_name
    if name in {"BINARY", "VARBINARY", "UNIQUEIDENTIFIER"}:
        if name == "UNIQUEIDENTIFIER":
            return "RAW(16)"
        if spec.length == "MAX":
            raise TypeMappingError(f"Unsupported Oracle target length for {spec.name}: MAX")
        return f"RAW({spec.length})" if spec.length is not None else "RAW"
    if name == "BIT":
        return "NUMBER(1,0)"
    if name in {"FLOAT", "REAL"}:
        return "BINARY_DOUBLE"
    if name == "DATE":
        return "DATE"
    if name in {"DATETIME", "DATETIME2", "SMALLDATETIME", "TIME"}:
        return "TIMESTAMP"
    if name == "DATETIMEOFFSET":
        return "TIMESTAMP WITH TIME ZONE"
    raise TypeMappingError(f"Unsupported Oracle target type: {spec.name}")


def _render_target(target_technology: str, spec: SqlTypeSpec) -> str:
    if target_technology == "sql_server":
        return _render_tsql(spec)
    if target_technology == "oracle":
        return _render_oracle(spec)
    raise TypeMappingError(f"Unsupported target technology: {target_technology}")


def map_catalog_column_type(
    *,
    source_technology: str,
    target_technology: str,
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
) -> dict[str, str]:
    """Map extracted source metadata into catalog column type fields."""
    source = _source_type_spec(source_technology, type_name, max_length, precision, scale)
    canonical = _canonical_from_source(source_technology, source)
    source_sql_type = _render_source(source_technology, source)
    sql_type = (
        source_sql_type
        if source_technology == "oracle" and target_technology == "oracle"
        else _render_target(target_technology, canonical)
    )
    return {
        "source_sql_type": source_sql_type,
        "canonical_tsql_type": _render_tsql(canonical),
        "sql_type": sql_type,
    }
