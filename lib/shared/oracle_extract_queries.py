from __future__ import annotations

from textwrap import dedent

from shared.setup_ddl_support.db_helpers import build_schema_in_clause

VALID_DEP_TYPES = ("PROCEDURE", "VIEW", "FUNCTION")


def definitions_object_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION')
          AND OWNER IN ({owners})
        ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
        """
    ).strip()


def view_text_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, VIEW_NAME, TEXT
        FROM ALL_VIEWS
        WHERE OWNER IN ({owners})
        ORDER BY OWNER, VIEW_NAME
        """
    ).strip()


def table_columns_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, TABLE_NAME, COLUMN_NAME, COLUMN_ID, DATA_TYPE,
               DATA_LENGTH, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, IDENTITY_COLUMN
        FROM ALL_TAB_COLUMNS
        WHERE OWNER IN ({owners})
        ORDER BY OWNER, TABLE_NAME, COLUMN_ID
        """
    ).strip()


def pk_unique_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT c.OWNER, c.TABLE_NAME, c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE,
               cc.COLUMN_NAME, cc.POSITION
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc ON cc.OWNER = c.OWNER
          AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
          AND cc.TABLE_NAME = c.TABLE_NAME
        WHERE c.CONSTRAINT_TYPE IN ('P', 'U')
          AND c.OWNER IN ({owners})
        ORDER BY c.OWNER, c.TABLE_NAME, c.CONSTRAINT_NAME, cc.POSITION
        """
    ).strip()


def foreign_keys_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT c.OWNER, c.TABLE_NAME, c.CONSTRAINT_NAME,
               cc.COLUMN_NAME, cc.POSITION,
               rc.OWNER AS REF_OWNER, rc.TABLE_NAME AS REF_TABLE_NAME,
               rcc.COLUMN_NAME AS REF_COLUMN_NAME
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc ON cc.OWNER = c.OWNER
          AND cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
        JOIN ALL_CONSTRAINTS rc ON rc.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME
          AND rc.OWNER = c.R_OWNER
        JOIN ALL_CONS_COLUMNS rcc ON rcc.OWNER = rc.OWNER
          AND rcc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
          AND rcc.POSITION = cc.POSITION
        WHERE c.CONSTRAINT_TYPE = 'R'
          AND c.OWNER IN ({owners})
        ORDER BY c.OWNER, c.TABLE_NAME, c.CONSTRAINT_NAME, cc.POSITION
        """
    ).strip()


def identity_columns_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, TABLE_NAME, COLUMN_NAME
        FROM ALL_TAB_COLUMNS
        WHERE IDENTITY_COLUMN = 'YES'
          AND OWNER IN ({owners})
        """
    ).strip()


def object_types_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'MATERIALIZED VIEW')
          AND OWNER IN ({owners})
          AND STATUS = 'VALID'
        ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
        """
    ).strip()


def invalid_object_types_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, OBJECT_NAME, OBJECT_TYPE, STATUS
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION', 'MATERIALIZED VIEW')
          AND OWNER IN ({owners})
          AND STATUS != 'VALID'
        ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME
        """
    ).strip()


def dmf_sql(schemas: list[str], dep_type: str) -> str:
    if dep_type not in VALID_DEP_TYPES:
        raise ValueError(f"dep_type must be one of {VALID_DEP_TYPES}, got: {dep_type!r}")
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, NAME, REFERENCED_OWNER, REFERENCED_NAME, REFERENCED_TYPE
        FROM ALL_DEPENDENCIES
        WHERE TYPE = '{dep_type}'
          AND REFERENCED_TYPE IN ('TABLE', 'VIEW', 'FUNCTION', 'PROCEDURE')
          AND OWNER IN ({owners})
        ORDER BY OWNER, NAME
        """
    ).strip()


def proc_params_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, OBJECT_NAME, ARGUMENT_NAME, DATA_TYPE, DATA_LENGTH,
               DATA_PRECISION, DATA_SCALE, IN_OUT, DEFAULTED
        FROM ALL_ARGUMENTS
        WHERE PACKAGE_NAME IS NULL
          AND ARGUMENT_NAME IS NOT NULL
          AND OWNER IN ({owners})
        ORDER BY OWNER, OBJECT_NAME, SEQUENCE
        """
    ).strip()


def packages_sql(schemas: list[str]) -> str:
    owners = build_schema_in_clause(schemas, uppercase=True)
    return dedent(
        f"""
        SELECT OWNER, PACKAGE_NAME, OBJECT_NAME,
               CASE WHEN DATA_TYPE IS NULL THEN 'PROCEDURE' ELSE 'FUNCTION' END AS MEMBER_TYPE
        FROM ALL_ARGUMENTS
        WHERE PACKAGE_NAME IS NOT NULL
          AND OWNER IN ({owners})
          AND ARGUMENT_NAME IS NULL
          AND DATA_LEVEL = 0
        GROUP BY OWNER, PACKAGE_NAME, OBJECT_NAME,
                 CASE WHEN DATA_TYPE IS NULL THEN 'PROCEDURE' ELSE 'FUNCTION' END
        ORDER BY OWNER, PACKAGE_NAME, OBJECT_NAME
        """
    ).strip()


__all__ = [
    "VALID_DEP_TYPES",
    "definitions_object_sql",
    "view_text_sql",
    "table_columns_sql",
    "pk_unique_sql",
    "foreign_keys_sql",
    "identity_columns_sql",
    "object_types_sql",
    "invalid_object_types_sql",
    "dmf_sql",
    "proc_params_sql",
    "packages_sql",
]
