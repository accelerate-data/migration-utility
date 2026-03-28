"""DDL export CLI.

Connects to a live SQL Server and writes GO-delimited DDL files compatible
with loader.load_directory() to a target directory.

Usage:
    uv run --extra export python export_ddl.py \\
      --host 127.0.0.1 --port 1433 --database MigrationTest \\
      --user sa --password P@ssw0rd123 \\
      --output /path/to/artifacts/ddl/

Requires pyodbc and the Microsoft ODBC Driver for SQL Server.
Install: uv sync --extra export
"""

from __future__ import annotations

import sys
from pathlib import Path

import typer

app = typer.Typer(add_completion=False)


def _connect(host: str, port: int, database: str, user: str, password: str):
    try:
        import pyodbc
    except ImportError:
        typer.echo(
            "pyodbc is not installed. Run: uv sync --extra export",
            err=True,
        )
        raise typer.Exit(1)

    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        typer.echo(
            "No SQL Server ODBC driver found. Install 'ODBC Driver 18 for SQL Server'.",
            err=True,
        )
        raise typer.Exit(1)

    driver = drivers[-1]  # prefer latest
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )
    import pyodbc  # noqa: PLC0415

    return pyodbc.connect(conn_str)


def _export_modules(conn, object_type_code: str, output_path: Path) -> int:
    """Write OBJECT_DEFINITION() DDL for all objects of the given type.

    object_type_code: 'P' = procedures, 'V' = views, 'FN'/'IF'/'TF' = functions.
    Returns count of objects written.
    """
    type_list = ", ".join(f"'{t}'" for t in object_type_code.split(","))
    sql = f"""
        SELECT
            SCHEMA_NAME(o.schema_id) AS schema_name,
            o.name AS object_name,
            OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        WHERE o.type IN ({type_list})
          AND o.is_ms_shipped = 0
        ORDER BY schema_name, object_name
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    blocks: list[str] = []
    for schema_name, object_name, definition in rows:
        if definition:
            blocks.append(definition.strip())

    output_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return len(blocks)


def _export_tables(conn, output_path: Path) -> int:
    """Reconstruct CREATE TABLE DDL from system catalog and write to output_path."""
    sql = """
        SELECT
            SCHEMA_NAME(t.schema_id) AS schema_name,
            t.name AS table_name,
            c.name AS column_name,
            c.column_id,
            tp.name AS type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity,
            ic.seed_value,
            ic.increment_value
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types tp ON tp.user_type_id = c.user_type_id
        LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE t.is_ms_shipped = 0
        ORDER BY schema_name, table_name, c.column_id
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()

    # Group by table
    tables: dict[tuple[str, str], list] = {}
    for row in rows:
        key = (row.schema_name, row.table_name)
        tables.setdefault(key, []).append(row)

    def _col_def(row) -> str:
        type_name = row.type_name.upper()
        if type_name in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "BINARY", "VARBINARY"):
            length = "MAX" if row.max_length == -1 else str(row.max_length // (2 if type_name.startswith("N") else 1))
            type_str = f"{type_name}({length})"
        elif type_name in ("DECIMAL", "NUMERIC"):
            type_str = f"{type_name}({row.precision},{row.scale})"
        elif type_name in ("FLOAT", "REAL"):
            type_str = type_name
        else:
            type_str = type_name

        identity = f" IDENTITY({row.seed_value},{row.increment_value})" if row.is_identity else ""
        nullable = " NOT NULL" if not row.is_nullable else " NULL"
        return f"    [{row.column_name}] {type_str}{identity}{nullable}"

    blocks: list[str] = []
    for (schema_name, table_name), cols in tables.items():
        col_defs = ",\n".join(_col_def(c) for c in cols)
        blocks.append(f"CREATE TABLE [{schema_name}].[{table_name}] (\n{col_defs}\n)")

    output_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return len(blocks)


@app.command()
def main(
    host: str = typer.Option("127.0.0.1", help="SQL Server host"),
    port: int = typer.Option(1433, help="SQL Server port"),
    database: str = typer.Option(..., help="Database name"),
    user: str = typer.Option("sa", help="SQL login username"),
    password: str = typer.Option(..., envvar="SA_PASSWORD", help="SQL login password"),
    output: Path = typer.Option(..., help="Output directory for DDL files"),
) -> None:
    """Export DDL from a SQL Server database to a loader-compatible directory."""
    output.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Connecting to {host}:{port}/{database} ...", err=True)
    conn = _connect(host, port, database, user, password)
    typer.echo("Connected.", err=True)

    counts: dict[str, int] = {}

    typer.echo("Exporting tables ...", err=True)
    counts["tables"] = _export_tables(conn, output / "tables.sql")

    typer.echo("Exporting procedures ...", err=True)
    counts["procedures"] = _export_modules(conn, "P", output / "procedures.sql")

    typer.echo("Exporting views ...", err=True)
    counts["views"] = _export_modules(conn, "V", output / "views.sql")

    typer.echo("Exporting functions ...", err=True)
    counts["functions"] = _export_modules(conn, "FN,IF,TF", output / "functions.sql")

    conn.close()

    typer.echo(f"\nExport complete → {output}", err=True)
    for kind, count in counts.items():
        typer.echo(f"  {kind}: {count}", err=True)


if __name__ == "__main__":
    app()
