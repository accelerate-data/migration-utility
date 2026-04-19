"""Oracle table, view, and procedure clone helpers."""

from __future__ import annotations

import logging
import re
from typing import Any


def _services():
    from shared.sandbox import oracle_services

    return oracle_services

logger = logging.getLogger(__name__)


class OracleCloneMixin:
    def _load_object_columns(
        self,
        source_cursor: Any,
        source_schema: str,
        object_name: str,
    ) -> list[dict[str, Any]]:
        source_cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE "
            "FROM ALL_TAB_COLUMNS "
            "WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2) "
            "ORDER BY COLUMN_ID",
            [source_schema, object_name],
        )
        return [
            {
                "name": row[0],
                "data_type": row[1],
                "data_length": row[2],
                "data_precision": row[3],
                "data_scale": row[4],
                "nullable": row[5] == "Y",
            }
            for row in source_cursor.fetchall()
        ]

    @staticmethod
    def _render_column_type(column: dict[str, Any]) -> str:
        data_type = str(column["data_type"]).upper()
        data_length = column.get("data_length")
        data_precision = column.get("data_precision")
        data_scale = column.get("data_scale")

        if data_type in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW"} and data_length:
            return f"{data_type}({int(data_length)})"
        if data_type == "NUMBER" and data_precision is not None:
            if data_scale is not None:
                return f"NUMBER({int(data_precision)},{int(data_scale)})"
            return f"NUMBER({int(data_precision)})"
        return data_type

    def _create_empty_table(
        self,
        sandbox_cursor: Any,
        sandbox_schema: str,
        object_name: str,
        columns: list[dict[str, Any]],
    ) -> None:
        rendered = [
            (
                f'"{column["name"]}" {self._render_column_type(column)} '
                f'{"NULL" if column["nullable"] else "NOT NULL"}'
            )
            for column in columns
        ]
        sandbox_cursor.execute(
            f'CREATE TABLE "{sandbox_schema}"."{object_name}" ({", ".join(rendered)})'
        )

    def _clone_tables(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structures from source to sandbox via explicit DDL."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT TABLE_NAME FROM ALL_TABLES "
            "WHERE OWNER = UPPER(:1) AND NESTED = 'NO' AND SECONDARY = 'N' "
            "ORDER BY TABLE_NAME",
            [source_schema],
        )
        table_names = [row[0] for row in source_cursor.fetchall()]

        for table_name in table_names:
            try:
                columns = self._load_object_columns(source_cursor, source_schema, table_name)
                self._create_empty_table(sandbox_cursor, sandbox_schema, table_name, columns)
                cloned.append(f"{source_schema}.{table_name}")
            except _services()._import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "TABLE_CLONE_FAILED",
                    "message": f"Failed to clone {source_schema}.{table_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_table_clone_failed sandbox=%s table=%s error=%s",
                    sandbox_schema,
                    table_name,
                    exc,
                )

        return cloned, errors

    def _clone_views(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone view definitions from source to sandbox schema."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT VIEW_NAME, TEXT FROM ALL_VIEWS "
            "WHERE OWNER = UPPER(:1) ORDER BY VIEW_NAME",
            [source_schema],
        )
        views = source_cursor.fetchall()

        for view_name, view_text in views:
            _services()._validate_oracle_identifier(view_name)
            ddl = (
                f'CREATE OR REPLACE VIEW "{sandbox_schema}"."{view_name}" AS '
                f"{view_text}"
            )
            try:
                sandbox_cursor.execute(ddl)
                cloned.append(f"{source_schema}.{view_name}")
            except _services()._import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "VIEW_CLONE_FAILED",
                    "message": f"Failed to clone view {source_schema}.{view_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_view_clone_failed sandbox=%s view=%s error=%s",
                    sandbox_schema,
                    view_name,
                    exc,
                )

        return cloned, errors

    def _clone_procedures(
        self,
        source_cursor: Any,
        sandbox_cursor: Any,
        sandbox_schema: str,
        source_schema: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone procedure definitions from source to sandbox schema."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []

        source_cursor.execute(
            "SELECT DISTINCT NAME FROM ALL_SOURCE "
            "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' ORDER BY NAME",
            [source_schema],
        )
        proc_names = [row[0] for row in source_cursor.fetchall()]

        for proc_name in proc_names:
            source_cursor.execute(
                "SELECT TEXT FROM ALL_SOURCE "
                "WHERE OWNER = UPPER(:1) AND TYPE = 'PROCEDURE' AND NAME = :2 "
                "ORDER BY LINE",
                [source_schema, proc_name],
            )
            lines = [row[0] for row in source_cursor.fetchall()]
            if not lines:
                errors.append({
                    "code": "PROC_DEFINITION_EMPTY",
                    "message": f"No source lines found for {source_schema}.{proc_name}",
                })
                continue

            full_source = "".join(lines)
            ddl = re.sub(
                rf"\bPROCEDURE\s+{re.escape(proc_name)}\b",
                f'PROCEDURE "{sandbox_schema}"."{proc_name}"',
                full_source,
                count=1,
                flags=re.IGNORECASE,
            )
            ddl = f"CREATE OR REPLACE {ddl.lstrip()}"

            try:
                sandbox_cursor.execute(ddl)
                cloned.append(f"{source_schema}.{proc_name}")
            except _services()._import_oracledb().DatabaseError as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone {source_schema}.{proc_name}: {exc}",
                })
                logger.debug(
                    "event=oracle_proc_clone_failed sandbox=%s proc=%s error=%s",
                    sandbox_schema,
                    proc_name,
                    exc,
                )

        return cloned, errors
