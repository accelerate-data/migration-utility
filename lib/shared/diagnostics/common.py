"""Registration facade for cross-dialect diagnostic checks."""

from __future__ import annotations

from shared.diagnostics.common_support.dependency_checks import (  # noqa: F401
    check_circular_reference,
    check_dependency_has_error,
    check_nested_view_chain,
    check_transitive_scope_leak,
)
from shared.diagnostics.common_support.graph import (  # noqa: F401
    _get_dep_fqns,
    _load_catalog_json,
)
from shared.diagnostics.common_support.object_checks import (  # noqa: F401
    _has_llm_recovery_statements,
    check_multi_table_read,
    check_multi_table_write,
    check_parse_error,
    check_stale_object,
    check_unsupported_syntax,
)
from shared.diagnostics.common_support.reference_checks import (  # noqa: F401
    check_missing_reference,
    check_out_of_scope_reference,
    check_remote_exec_unsupported,
)

__all__ = [
    "_get_dep_fqns",
    "_has_llm_recovery_statements",
    "_load_catalog_json",
    "check_circular_reference",
    "check_dependency_has_error",
    "check_missing_reference",
    "check_multi_table_read",
    "check_multi_table_write",
    "check_nested_view_chain",
    "check_out_of_scope_reference",
    "check_parse_error",
    "check_remote_exec_unsupported",
    "check_stale_object",
    "check_transitive_scope_leak",
    "check_unsupported_syntax",
]
