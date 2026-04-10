"""Compatibility barrel for setup-ddl staging helpers."""

from shared.setup_ddl_support.staging_io import (
    load_staging_catalog_inputs,
    read_json,
    read_json_optional,
)
from shared.setup_ddl_support.staging_signals import (
    TYPE_MAPPING,
    apply_change_capture_rows,
    apply_column_rows,
    apply_fk_rows,
    apply_identity_rows,
    apply_pk_unique_rows,
    apply_sensitivity_rows,
    build_catalog_write_inputs,
    build_function_subtypes,
    build_long_truncation_map,
    build_object_types_map,
    build_proc_params,
    build_routing_flags,
    build_view_columns_map,
    build_view_definitions_map,
    ensure_table_skeleton,
)

__all__ = [
    "TYPE_MAPPING",
    "apply_change_capture_rows",
    "apply_column_rows",
    "apply_fk_rows",
    "apply_identity_rows",
    "apply_pk_unique_rows",
    "apply_sensitivity_rows",
    "build_catalog_write_inputs",
    "build_function_subtypes",
    "build_long_truncation_map",
    "build_object_types_map",
    "build_proc_params",
    "build_routing_flags",
    "build_view_columns_map",
    "build_view_definitions_map",
    "ensure_table_skeleton",
    "load_staging_catalog_inputs",
    "read_json",
    "read_json_optional",
]
