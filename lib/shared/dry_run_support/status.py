from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from shared.catalog import detect_object_type, load_proc_catalog, load_table_catalog, load_view_catalog
from shared.catalog_models import TableCatalog
from shared.dry_run_support.common import display_scope_status, read_catalog_json
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.dry_run import ObjectStatus, StageStatuses, StatusOutput, StatusSummary

logger = logging.getLogger(__name__)


def _single_object_status(
    project_root: Path,
    norm_fqn: str,
    *,
    obj_type: str | None = None,
    cat_data: dict[str, Any] | None = None,
) -> ObjectStatus | StatusOutput:
    """Collate all stage statuses for a single object."""
    obj_type = obj_type or detect_object_type(project_root, norm_fqn)
    if obj_type is None:
        return StatusOutput(
            fqn=norm_fqn,
            type=None,
            stages=None,
        )
    scope = profile = test_gen = refactor = generate = None

    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            logger.warning(
                "event=view_catalog_load_failed fqn=%s", norm_fqn,
            )
            cat = None
        scope = display_scope_status(cat.scoping.status if cat and cat.scoping else None)
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor = cat.refactor.status if cat and cat.refactor else None
        generate = cat.generate.status if cat and cat.generate else None
    else:
        if cat_data is not None:
            try:
                cat = TableCatalog.model_validate(cat_data)
            except ValidationError:
                cat = None
        else:
            try:
                cat = load_table_catalog(project_root, norm_fqn)
            except (json.JSONDecodeError, OSError, CatalogLoadError):
                cat = None
        if cat and (cat.is_source or cat.is_seed):
            return ObjectStatus(
                fqn=norm_fqn,
                type=obj_type,
                stages=StageStatuses(
                    scope="N/A",
                    profile="N/A",
                    test_gen="N/A",
                    refactor="N/A",
                    generate="N/A",
                ),
            )
        scope = display_scope_status(cat.scoping.status if cat and cat.scoping else None)
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor_status: str | None = cat.refactor.status if cat and cat.refactor else None
        if refactor_status is None:
            writer = cat.scoping.selected_writer if cat and cat.scoping else None
            if writer:
                try:
                    proc_cat = load_proc_catalog(project_root, normalize(writer))
                except (json.JSONDecodeError, OSError, CatalogLoadError):
                    proc_cat = None
                refactor_status = (
                    proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
                )
        refactor = refactor_status
        generate = cat.generate.status if cat and cat.generate else None

    return ObjectStatus(
        fqn=norm_fqn,
        type=obj_type,
        stages=StageStatuses(
            scope=scope,
            profile=profile,
            test_gen=test_gen,
            refactor=refactor,
            generate=generate,
        ),
    )


def run_status(
    project_root: Path,
    fqn: str | None = None,
) -> StatusOutput | ObjectStatus:
    """Collate CLI-written statuses from catalog files."""
    if fqn is not None:
        return _single_object_status(project_root, normalize(fqn))

    catalog_dir = project_root / "catalog"
    objects: list[ObjectStatus] = []
    stage_counts: dict[str, dict[str, int]] = {
        stage: {} for stage in ("scope", "profile", "test_gen", "refactor", "generate")
    }

    for bucket in ("tables", "views"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for path in sorted(bucket_dir.glob("*.json")):
            norm_fqn = path.stem
            if bucket == "tables":
                try:
                    cat_data = read_catalog_json(path)
                except CatalogLoadError:
                    cat_data = {}
                if cat_data.get("is_source") or cat_data.get("is_seed"):
                    continue
                obj_status = _single_object_status(
                    project_root,
                    norm_fqn,
                    obj_type="table",
                    cat_data=cat_data,
                )
            else:
                obj_status = _single_object_status(project_root, norm_fqn)

            if not isinstance(obj_status, ObjectStatus):
                continue

            objects.append(obj_status)
            for stage_name, status_val in obj_status.stages.model_dump().items():
                label = status_val if status_val else "pending"
                stage_counts[stage_name][label] = (
                    stage_counts[stage_name].get(label, 0) + 1
                )

    return StatusOutput(
        objects=objects,
        summary=StatusSummary(total=len(objects), by_stage=stage_counts),
    )
