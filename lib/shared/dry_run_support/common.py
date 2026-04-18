from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.loader_data import CatalogLoadError
from shared.output_models.dry_run import ObjectReadiness, ReadinessDetail
from shared.runtime_config import get_runtime_role

logger = logging.getLogger(__name__)

VALID_STAGES = frozenset(
    {"setup-ddl", "scope", "profile", "test-gen", "refactor", "generate"}
)
RESETTABLE_STAGES = frozenset({"scope", "profile", "generate-tests", "refactor"})
RESET_GLOBAL_PATHS = ("catalog", "ddl", ".staging", "test-specs", "dbt")
RESET_PRESERVE_CATALOG_PATHS = ("dbt", "test-specs", ".staging", ".migration-runs")
RESET_PRESERVE_CATALOG_SECTIONS_BY_BUCKET: dict[str, tuple[str, tuple[str, ...]]] = {
    "tables": ("table", ("test_gen", "generate", "refactor")),
    "views": ("view", ("test_gen", "generate", "refactor")),
    "procedures": ("procedure", ("refactor",)),
    "functions": ("function", ()),
}
RESET_GLOBAL_MANIFEST_SECTIONS = (
    "runtime.source",
    "runtime.target",
    "runtime.sandbox",
    "extraction",
    "init_handoff",
)

_RESET_STAGE_SECTIONS: dict[str, tuple[str, ...]] = {
    "scope": ("scoping", "profile", "test_gen"),
    "profile": ("profile", "test_gen"),
    "generate-tests": ("test_gen",),
    "refactor": (),
}


def display_scope_status(scope_status: str | None) -> str | None:
    """Map completed internal scope states to the unified /status display value."""
    if scope_status in {"resolved", "analyzed"}:
        return "ok"
    return scope_status


def detail(ready: bool, reason: str, code: str | None = None) -> ReadinessDetail:
    return ReadinessDetail(ready=ready, reason=reason, code=code)


def object_detail(
    object_fqn: str,
    object_type: str | None,
    ready: bool,
    reason: str,
    code: str | None = None,
    *,
    not_applicable: bool | None = None,
) -> ObjectReadiness:
    return ObjectReadiness(
        object=object_fqn,
        object_type=object_type,
        ready=ready,
        reason=reason,
        code=code,
        not_applicable=not_applicable,
    )


def runtime_role_is_configured(manifest: dict[str, Any], role: str) -> bool:
    runtime_role = get_runtime_role(manifest, role)
    if runtime_role is None:
        return False
    connection = runtime_role.connection.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    return bool(connection)


def load_manifest_json(manifest_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "event=manifest_parse_failed path=%s error=%s",
            manifest_path, exc,
        )
        return None


def read_catalog_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise CatalogLoadError(str(path), exc) from exc
