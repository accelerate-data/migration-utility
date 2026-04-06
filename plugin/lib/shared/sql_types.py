"""SQL type formatting utilities.

Single source of truth for formatting column types from sys.columns (SQL Server)
and ALL_TAB_COLUMNS (Oracle) metadata.
"""

from __future__ import annotations


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
        return f"{tn}({precision},{scale})"
    if tn == "NUMBER":
        return f"NUMBER({precision},{scale})" if precision > 0 else "NUMBER"
    return tn
