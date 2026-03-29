"""DDL export CLI.

Connects to a live SQL Server and writes GO-delimited DDL files compatible
with loader.load_directory() to a target directory.  Optionally extracts
catalog signals and DMF reference data into per-object JSON files.

Usage:
    uv run --extra export python export_ddl.py \\
      --host 127.0.0.1 --port 1433 --database MigrationTest \\
      --user sa --password P@ssw0rd123 \\
      --output /path/to/artifacts/ddl/ \\
      --catalog

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
            CAST(ic.seed_value AS BIGINT) AS seed_value,
            CAST(ic.increment_value AS BIGINT) AS increment_value
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


def _extract_table_columns(conn) -> dict[str, list[dict]]:
    """Extract column definitions for all tables.

    Returns {normalized_fqn: [{name, sql_type, is_nullable, is_identity}]}.
    Reuses the same sys.columns query as _export_tables.
    """
    from shared.name_resolver import normalize

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
            c.is_identity
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types tp ON tp.user_type_id = c.user_type_id
        WHERE t.is_ms_shipped = 0
        ORDER BY schema_name, table_name, c.column_id
    """
    cursor = conn.cursor()
    cursor.execute(sql)

    def _format_type(row) -> str:
        type_name = row.type_name.upper()
        if type_name in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "BINARY", "VARBINARY"):
            length = "MAX" if row.max_length == -1 else str(row.max_length // (2 if type_name.startswith("N") else 1))
            return f"{type_name}({length})"
        if type_name in ("DECIMAL", "NUMERIC"):
            return f"{type_name}({row.precision},{row.scale})"
        return type_name

    result: dict[str, list[dict]] = {}
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.table_name}")
        if fqn not in result:
            result[fqn] = []
        result[fqn].append({
            "name": row.column_name,
            "sql_type": _format_type(row),
            "is_nullable": bool(row.is_nullable),
            "is_identity": bool(row.is_identity),
        })
    return result


def _extract_proc_params(conn) -> dict[str, list[dict]]:
    """Extract procedure parameter definitions.

    Returns {normalized_fqn: [{name, sql_type, is_output, has_default}]}.
    """
    from shared.name_resolver import normalize

    sql = """
        SELECT
            SCHEMA_NAME(o.schema_id) AS schema_name,
            o.name AS proc_name,
            p.name AS param_name,
            TYPE_NAME(p.user_type_id) AS type_name,
            p.max_length,
            p.precision,
            p.scale,
            p.is_output,
            p.has_default_value
        FROM sys.parameters p
        JOIN sys.objects o ON o.object_id = p.object_id
        WHERE o.type = 'P' AND o.is_ms_shipped = 0 AND p.parameter_id > 0
        ORDER BY schema_name, proc_name, p.parameter_id
    """
    cursor = conn.cursor()
    cursor.execute(sql)

    def _format_type(row) -> str:
        type_name = row.type_name.upper()
        if type_name in ("NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "BINARY", "VARBINARY"):
            length = "MAX" if row.max_length == -1 else str(row.max_length // (2 if type_name.startswith("N") else 1))
            return f"{type_name}({length})"
        if type_name in ("DECIMAL", "NUMERIC"):
            return f"{type_name}({row.precision},{row.scale})"
        return type_name

    result: dict[str, list[dict]] = {}
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.proc_name}")
        if fqn not in result:
            result[fqn] = []
        result[fqn].append({
            "name": row.param_name,
            "sql_type": _format_type(row),
            "is_output": bool(row.is_output),
            "has_default": bool(row.has_default_value),
        })
    return result


def _extract_table_signals(conn) -> dict[str, dict]:
    """Extract catalog signals for all tables: PKs, unique indexes, FKs, identity, CDC, change tracking, sensitivity."""
    from shared.name_resolver import normalize

    signals: dict[str, dict] = {}

    # PKs and unique indexes
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            SCHEMA_NAME(t.schema_id) AS schema_name,
            t.name AS table_name,
            i.name AS index_name,
            i.is_unique,
            i.is_primary_key,
            c.name AS column_name,
            ic.key_ordinal
        FROM sys.tables t
        JOIN sys.indexes i ON i.object_id = t.object_id AND (i.is_primary_key = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0))
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE t.is_ms_shipped = 0
        ORDER BY schema_name, table_name, i.index_id, ic.key_ordinal
    """)
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.table_name}")
        if fqn not in signals:
            signals[fqn] = {
                "primary_keys": [], "unique_indexes": [], "foreign_keys": [],
                "auto_increment_columns": [], "change_capture": None,
                "sensitivity_classifications": [],
            }
        sig = signals[fqn]
        if row.is_primary_key:
            existing = next((pk for pk in sig["primary_keys"] if pk["constraint_name"] == row.index_name), None)
            if existing is None:
                sig["primary_keys"].append({"constraint_name": row.index_name, "columns": [row.column_name]})
            else:
                existing["columns"].append(row.column_name)
        else:
            existing = next((ui for ui in sig["unique_indexes"] if ui["index_name"] == row.index_name), None)
            if existing is None:
                sig["unique_indexes"].append({"index_name": row.index_name, "columns": [row.column_name]})
            else:
                existing["columns"].append(row.column_name)

    # Foreign keys
    cursor.execute("""
        SELECT
            SCHEMA_NAME(t.schema_id) AS schema_name,
            t.name AS table_name,
            fk.name AS constraint_name,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
            SCHEMA_NAME(rt.schema_id) AS ref_schema,
            rt.name AS ref_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
        FROM sys.foreign_keys fk
        JOIN sys.tables t ON t.object_id = fk.parent_object_id
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY schema_name, table_name, fk.name, fkc.constraint_column_id
    """)
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.table_name}")
        if fqn not in signals:
            signals[fqn] = {
                "primary_keys": [], "unique_indexes": [], "foreign_keys": [],
                "auto_increment_columns": [], "change_capture": None,
                "sensitivity_classifications": [],
            }
        sig = signals[fqn]
        existing = next((f for f in sig["foreign_keys"] if f["constraint_name"] == row.constraint_name), None)
        if existing is None:
            sig["foreign_keys"].append({
                "constraint_name": row.constraint_name,
                "columns": [row.column_name],
                "referenced_schema": row.ref_schema,
                "referenced_table": row.ref_table,
                "referenced_columns": [row.ref_column],
            })
        else:
            existing["columns"].append(row.column_name)
            existing["referenced_columns"].append(row.ref_column)

    # Identity columns
    cursor.execute("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, c.name AS column_name,
               CAST(c.seed_value AS BIGINT) AS seed_value,
               CAST(c.increment_value AS BIGINT) AS increment_value
        FROM sys.identity_columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        WHERE t.is_ms_shipped = 0
    """)
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.table_name}")
        if fqn not in signals:
            signals[fqn] = {
                "primary_keys": [], "unique_indexes": [], "foreign_keys": [],
                "auto_increment_columns": [], "change_capture": None,
                "sensitivity_classifications": [],
            }
        signals[fqn]["auto_increment_columns"].append({
            "column": row.column_name,
            "mechanism": "identity",
            "seed": row.seed_value,
            "increment": row.increment_value,
        })

    # CDC
    cursor.execute("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, t.is_tracked_by_cdc
        FROM sys.tables t WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1
    """)
    for row in cursor.fetchall():
        fqn = normalize(f"{row.schema_name}.{row.table_name}")
        if fqn in signals:
            signals[fqn]["change_capture"] = {"enabled": True, "mechanism": "cdc"}

    # Change tracking (graceful)
    try:
        cursor.execute("""
            SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
            FROM sys.change_tracking_tables ct
            JOIN sys.tables t ON t.object_id = ct.object_id
        """)
        for row in cursor.fetchall():
            fqn = normalize(f"{row.schema_name}.{row.table_name}")
            if fqn in signals:
                signals[fqn]["change_capture"] = {"enabled": True, "mechanism": "change_tracking"}
    except Exception:
        pass  # change tracking not available

    # Sensitivity classifications (graceful)
    try:
        cursor.execute("""
            SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
                   sc.label, sc.information_type, COL_NAME(sc.major_id, sc.minor_id) AS column_name
            FROM sys.sensitivity_classifications sc
            JOIN sys.tables t ON t.object_id = sc.major_id
            WHERE t.is_ms_shipped = 0
        """)
        for row in cursor.fetchall():
            fqn = normalize(f"{row.schema_name}.{row.table_name}")
            if fqn in signals:
                signals[fqn]["sensitivity_classifications"].append({
                    "column": row.column_name,
                    "label": row.label,
                    "information_type": row.information_type,
                })
    except Exception:
        pass  # sensitivity classifications not available (requires SQL Server 2019+)

    return signals


def _extract_dmf_refs(conn, object_type_code: str) -> list[dict]:
    """Run server-side cursor to call sys.dm_sql_referenced_entities for all objects of a type.

    Returns raw DMF result rows as dicts.
    """
    type_filter = " OR ".join(f"o.type = '{t}'" for t in object_type_code.split(","))
    sql = f"""
        SET NOCOUNT ON;
        DECLARE @result TABLE (
            referencing_schema NVARCHAR(128), referencing_name NVARCHAR(128),
            referenced_schema NVARCHAR(128), referenced_entity NVARCHAR(128),
            referenced_minor_name NVARCHAR(128), referenced_class_desc NVARCHAR(60),
            is_selected BIT, is_updated BIT, is_select_all BIT,
            is_insert_all BIT, is_all_columns_found BIT,
            is_caller_dependent BIT, is_ambiguous BIT,
            referenced_database_name NVARCHAR(128),
            referenced_server_name NVARCHAR(128)
        );
        DECLARE @schema NVARCHAR(128), @name NVARCHAR(128);
        DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT SCHEMA_NAME(o.schema_id), o.name FROM sys.objects o
            WHERE ({type_filter}) AND o.is_ms_shipped = 0;
        OPEN cur;
        FETCH NEXT FROM cur INTO @schema, @name;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                INSERT INTO @result
                SELECT @schema, @name,
                    ISNULL(ref.referenced_schema_name, ''),
                    ISNULL(ref.referenced_entity_name, ''),
                    ISNULL(ref.referenced_minor_name, ''),
                    ISNULL(ref.referenced_class_desc, ''),
                    ISNULL(ref.is_selected, 0), ISNULL(ref.is_updated, 0),
                    ISNULL(ref.is_select_all, 0), ISNULL(ref.is_insert_all, 0),
                    ISNULL(ref.is_all_columns_found, 0),
                    ISNULL(ref.is_caller_dependent, 0), ISNULL(ref.is_ambiguous, 0),
                    ISNULL(ref.referenced_database_name, ''),
                    ISNULL(ref.referenced_server_name, '')
                FROM sys.dm_sql_referenced_entities(
                    QUOTENAME(@schema) + '.' + QUOTENAME(@name), 'OBJECT'
                ) ref;
            END TRY
            BEGIN CATCH
            END CATCH
            FETCH NEXT FROM cur INTO @schema, @name;
        END;
        CLOSE cur; DEALLOCATE cur;
        SELECT * FROM @result;
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    # Advance past any intermediate result sets (e.g. from WHILE loop row counts)
    while cursor.description is None:
        if not cursor.nextset():
            return []
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append(dict(zip(columns, row)))
    return rows


def _build_object_type_map(conn) -> dict[str, str]:
    """Return {normalized_fqn: catalog_bucket} for all non-system objects.

    Used to resolve OBJECT_OR_COLUMN references in DMF rows.
    """
    from shared.name_resolver import normalize

    mapping = {
        "U": "tables",
        "V": "views",
        "P": "procedures",
        "FN": "functions",
        "IF": "functions",
        "TF": "functions",
    }
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type
        FROM sys.objects o
        WHERE o.is_ms_shipped = 0
          AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
    """)
    result: dict[str, str] = {}
    for row in cursor.fetchall():
        fqn = normalize(f"{row[0]}.{row[1]}")
        bucket = mapping.get(row[2].strip())
        if bucket:
            result[fqn] = bucket
    return result


def _scan_routing_flags(conn) -> dict[str, dict[str, bool]]:
    """Scan all proc/view/function bodies for routing flags.

    Returns {normalized_fqn: {"needs_llm": bool, "needs_enrich": bool}}.
    Only includes objects where at least one flag is True.
    """
    from shared.catalog import scan_routing_flags as _check
    from shared.name_resolver import normalize

    flags: dict[str, dict[str, bool]] = {}
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name,
               OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        WHERE o.type IN ('P', 'V', 'FN', 'IF', 'TF') AND o.is_ms_shipped = 0
    """)
    for row in cursor.fetchall():
        if row.definition:
            fqn = normalize(f"{row.schema_name}.{row.object_name}")
            result = _check(row.definition)
            if result["needs_llm"] or result["needs_enrich"]:
                flags[fqn] = result
    return flags


@app.command()
def main(
    host: str = typer.Option("127.0.0.1", help="SQL Server host"),
    port: int = typer.Option(1433, help="SQL Server port"),
    database: str = typer.Option(..., help="Database name"),
    user: str = typer.Option("sa", help="SQL login username"),
    password: str = typer.Option(..., envvar="SA_PASSWORD", help="SQL login password"),
    output: Path = typer.Option(..., help="Output directory for DDL files"),
    catalog: bool = typer.Option(False, help="Also extract catalog signals and DMF references"),
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

    if catalog:
        from shared.catalog import write_catalog_files
        from shared.name_resolver import normalize

        typer.echo("Extracting table columns ...", err=True)
        table_columns = _extract_table_columns(conn)
        typer.echo(f"  {sum(len(v) for v in table_columns.values())} columns across {len(table_columns)} tables", err=True)

        typer.echo("Extracting procedure parameters ...", err=True)
        proc_params = _extract_proc_params(conn)
        typer.echo(f"  {sum(len(v) for v in proc_params.values())} params across {len(proc_params)} procedures", err=True)

        typer.echo("Extracting catalog signals ...", err=True)
        table_signals = _extract_table_signals(conn)
        # Merge columns into table_signals
        for fqn, cols in table_columns.items():
            if fqn not in table_signals:
                table_signals[fqn] = {}
            table_signals[fqn]["columns"] = cols

        object_type_map = _build_object_type_map(conn)

        typer.echo("Scanning proc bodies for routing flags ...", err=True)
        rflags = _scan_routing_flags(conn)
        llm_count = sum(1 for v in rflags.values() if v.get("needs_llm"))
        enrich_count = sum(1 for v in rflags.values() if v.get("needs_enrich"))
        typer.echo(f"  {llm_count} need LLM, {enrich_count} need enrichment", err=True)

        typer.echo("Extracting procedure references (DMF) ...", err=True)
        proc_rows = _extract_dmf_refs(conn, "P")
        typer.echo(f"  {len(proc_rows)} DMF rows from procedures", err=True)

        typer.echo("Extracting view references (DMF) ...", err=True)
        view_rows = _extract_dmf_refs(conn, "V")
        typer.echo(f"  {len(view_rows)} DMF rows from views", err=True)

        typer.echo("Extracting function references (DMF) ...", err=True)
        func_rows = _extract_dmf_refs(conn, "FN,IF,TF")
        typer.echo(f"  {len(func_rows)} DMF rows from functions", err=True)

        cat_counts = write_catalog_files(
            output,
            table_signals=table_signals,
            proc_dmf_rows=proc_rows,
            view_dmf_rows=view_rows,
            func_dmf_rows=func_rows,
            routing_flags=rflags,
            database=database,
            object_types=object_type_map,
            proc_params=proc_params,
        )
        typer.echo(f"\nCatalog files written:", err=True)
        for kind, count in cat_counts.items():
            typer.echo(f"  catalog/{kind}: {count}", err=True)

    conn.close()

    typer.echo(f"\nExport complete → {output}", err=True)
    for kind, count in counts.items():
        typer.echo(f"  {kind}: {count}", err=True)


if __name__ == "__main__":
    app()
