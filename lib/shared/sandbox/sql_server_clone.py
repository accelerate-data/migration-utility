"""SQL Server table, view, and procedure clone helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


def _services():
    from shared.sandbox import sql_server_services

    return sql_server_services

if TYPE_CHECKING:
    import pyodbc


class SqlServerCloneMixin:
    def _load_object_columns(
        self,
        source_cursor: pyodbc.Cursor,
        schema_name: str,
        object_name: str,
    ) -> list[dict[str, Any]]:
        identity_columns = _services()._get_identity_columns(
            source_cursor, f"[{schema_name}].[{object_name}]",
        )
        source_cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
            "NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            schema_name,
            object_name,
        )
        return [
            {
                "name": row[0],
                "data_type": row[1],
                "char_len": row[2],
                "precision": row[3],
                "scale": row[4],
                "datetime_precision": row[5],
                "nullable": row[6] == "YES",
                "identity": row[0] in identity_columns,
            }
            for row in source_cursor.fetchall()
        ]

    @staticmethod
    def _render_column_type(column: dict[str, Any]) -> str:
        data_type = str(column["data_type"]).lower()
        char_len = column.get("char_len")
        precision = column.get("precision")
        scale = column.get("scale")
        datetime_precision = column.get("datetime_precision")

        if data_type in {"varchar", "nvarchar", "char", "nchar", "binary", "varbinary"}:
            if char_len in (-1, None):
                return f"{data_type}(MAX)"
            return f"{data_type}({int(char_len)})"
        if data_type in {"decimal", "numeric"} and precision is not None:
            return f"{data_type}({int(precision)},{int(scale or 0)})"
        if data_type in {"datetime2", "datetimeoffset", "time"} and datetime_precision is not None:
            return f"{data_type}({int(datetime_precision)})"
        return data_type

    def _create_empty_table(
        self,
        sandbox_cursor: pyodbc.Cursor,
        schema_name: str,
        object_name: str,
        columns: list[dict[str, Any]],
    ) -> None:
        rendered = []
        for column in columns:
            line = f"[{column['name']}] {self._render_column_type(column)}"
            if column["identity"]:
                line += " IDENTITY(1,1)"
            line += " NULL" if column["nullable"] else " NOT NULL"
            rendered.append(line)
        sandbox_cursor.execute(
            f"CREATE TABLE [{schema_name}].[{object_name}] ({', '.join(rendered)})"
        )

    def _clone_tables(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structure from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ("
            + placeholders
            + ") ORDER BY TABLE_SCHEMA, TABLE_NAME",
            *schemas,
        )
        for schema_name, table_name in source_cursor.fetchall():
            _services()._validate_identifier(schema_name)
            _services()._validate_identifier(table_name)
            fqn = f"[{schema_name}].[{table_name}]"
            try:
                columns = self._load_object_columns(source_cursor, schema_name, table_name)
                self._create_empty_table(sandbox_cursor, schema_name, table_name, columns)
                cloned.append(f"{schema_name}.{table_name}")
            except _services()._import_pyodbc().Error as exc:
                errors.append({
                    "code": "TABLE_CLONE_FAILED",
                    "message": f"Failed to clone {fqn}: {exc}",
                })
        return cloned, errors

    def _clone_views(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone view definitions from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.VIEWS "
            "WHERE TABLE_SCHEMA IN ("
            + placeholders
            + ") ORDER BY TABLE_SCHEMA, TABLE_NAME",
            *schemas,
        )
        for schema_name, view_name in source_cursor.fetchall():
            _services()._validate_identifier(schema_name)
            _services()._validate_identifier(view_name)
            fqn = f"[{schema_name}].[{view_name}]"
            source_cursor.execute(
                "SELECT OBJECT_DEFINITION(OBJECT_ID(?))",
                f"{schema_name}.{view_name}",
            )
            rows = source_cursor.fetchall()
            definition = rows[0][0] if rows else None
            if definition is None:
                errors.append({
                    "code": "VIEW_DEFINITION_NULL",
                    "message": f"Cannot read definition for {fqn} (encrypted or inaccessible)",
                })
                continue
            try:
                sandbox_cursor.execute(definition)
                cloned.append(f"{schema_name}.{view_name}")
            except _services()._import_pyodbc().Error as exc:
                errors.append({
                    "code": "VIEW_CLONE_FAILED",
                    "message": f"Failed to clone view {fqn}: {exc}",
                })
        return cloned, errors

    def _clone_procedures(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone procedure definitions from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        placeholders = ",".join("?" for _ in schemas)
        source_cursor.execute(
            "SELECT s.name AS schema_name, p.name AS proc_name, "
            "       OBJECT_DEFINITION(p.object_id) AS definition "
            "FROM sys.procedures p "
            "JOIN sys.schemas s ON p.schema_id = s.schema_id "
            "WHERE s.name IN ("
            + placeholders
            + ") ORDER BY s.name, p.name",
            *schemas,
        )
        for schema_name, proc_name, definition in source_cursor.fetchall():
            _services()._validate_identifier(schema_name)
            _services()._validate_identifier(proc_name)
            fqn = f"{schema_name}.{proc_name}"
            if definition is None:
                errors.append({
                    "code": "PROC_DEFINITION_NULL",
                    "message": f"Cannot read definition for {fqn} (encrypted or inaccessible)",
                })
                continue
            try:
                sandbox_cursor.execute(definition)
                cloned.append(fqn)
            except _services()._import_pyodbc().Error as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone procedure {fqn}: {exc}",
                })
        return cloned, errors
