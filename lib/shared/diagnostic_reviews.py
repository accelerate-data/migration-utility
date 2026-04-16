"""Reviewed catalog diagnostic support."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.catalog import write_json
from shared.env_config import resolve_catalog_dir

REVIEW_ARTIFACT = "diagnostic-reviews.json"
REVIEW_SCHEMA_VERSION = "1.0"

ObjectType = Literal["table", "view", "mv"]


class DiagnosticIdentity(BaseModel):
    """Stable identity for one active catalog diagnostic."""

    fqn: str
    object_type: ObjectType = "table"
    code: str
    message_hash: str


class ReviewedDiagnostic(DiagnosticIdentity):
    """One reviewed warning persisted under catalog/."""

    status: Literal["accepted"] = "accepted"
    reason: str
    evidence: list[str] = Field(default_factory=list)
    reviewed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    reviewed_by: str = "agent"


def review_artifact_path(project_root: Path) -> Path:
    """Return the project-local reviewed diagnostic artifact path."""
    return resolve_catalog_dir(project_root) / REVIEW_ARTIFACT


def _message_hash(message: str | None) -> str:
    raw = (message or "").strip().encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def diagnostic_identity(
    fqn: str,
    diagnostic: dict[str, Any],
    *,
    object_type: ObjectType = "table",
) -> DiagnosticIdentity:
    """Build a stable diagnostic identity from a catalog diagnostic entry."""
    code = diagnostic.get("code")
    message = diagnostic.get("message")
    return DiagnosticIdentity(
        fqn=fqn,
        object_type=object_type,
        code=code if isinstance(code, str) and code else "UNKNOWN",
        message_hash=_message_hash(message if isinstance(message, str) else None),
    )


def load_reviewed_diagnostics(project_root: Path) -> list[ReviewedDiagnostic]:
    """Load reviewed warnings from catalog/diagnostic-reviews.json."""
    path = review_artifact_path(project_root)
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return [
        ReviewedDiagnostic.model_validate(item)
        for item in data.get("reviews", [])
    ]


def write_reviewed_diagnostic(project_root: Path, review: ReviewedDiagnostic) -> Path:
    """Upsert a reviewed warning entry and return the artifact path."""
    path = review_artifact_path(project_root)
    reviews = load_reviewed_diagnostics(project_root)
    replacement_key = (review.fqn, review.object_type, review.code, review.message_hash)
    kept = [
        existing
        for existing in reviews
        if (existing.fqn, existing.object_type, existing.code, existing.message_hash) != replacement_key
    ]
    payload = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "reviews": [
            item.model_dump(mode="json", exclude_none=True)
            for item in [*kept, review]
        ],
    }
    write_json(path, payload)
    return path


def partition_reviewed_warnings(
    project_root: Path,
    *,
    fqn: str,
    object_type: ObjectType,
    warnings: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Return visible warnings and the count hidden by accepted reviews."""
    accepted = {
        (review.fqn, review.object_type, review.code, review.message_hash)
        for review in load_reviewed_diagnostics(project_root)
        if review.status == "accepted"
    }
    visible: list[dict[str, Any]] = []
    hidden = 0
    for warning in warnings:
        identity = diagnostic_identity(fqn, warning, object_type=object_type)
        key = (identity.fqn, identity.object_type, identity.code, identity.message_hash)
        if key in accepted:
            hidden += 1
        else:
            visible.append(warning)
    return visible, hidden
