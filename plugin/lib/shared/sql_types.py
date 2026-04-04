"""SQL type formatting utilities.

Single source of truth for formatting SQL Server column types from
sys.columns metadata (type_name, max_length, precision, scale).
"""

from __future__ import annotations


def format_sql_type(
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
) -> str:
    """Format a SQL Server column type string from sys.columns metadata.

    Rules:
        NVARCHAR/VARCHAR/NCHAR/CHAR: TYPE(MAX) if -1, else length (halved for N-types)
        BINARY/VARBINARY: TYPE(MAX) if -1, else max_length
        DECIMAL/NUMERIC: TYPE(precision, scale)
        FLOAT/REAL: bare type
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
    if tn in ("DECIMAL", "NUMERIC"):
        return f"{tn}({precision},{scale})"
    return tn
