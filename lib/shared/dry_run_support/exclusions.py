from __future__ import annotations

import json
import logging
from pathlib import Path

from shared.catalog import detect_catalog_bucket, write_json
from shared.name_resolver import normalize
from shared.output_models.dry_run import ExcludeOutput

logger = logging.getLogger(__name__)


def run_exclude(project_root: Path, fqns: list[str]) -> ExcludeOutput:
    """Set ``excluded: true`` on each named table or view catalog file."""
    marked: list[str] = []
    not_found: list[str] = []

    for raw_fqn in fqns:
        norm = normalize(raw_fqn)
        bucket = detect_catalog_bucket(project_root, norm)
        if bucket is None:
            logger.warning(
                "event=exclude_not_found component=exclude operation=run_exclude fqn=%s",
                norm,
            )
            not_found.append(norm)
            continue

        catalog_path = project_root / "catalog" / bucket / f"{norm}.json"
        try:
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "event=exclude_read_error component=exclude operation=run_exclude "
                "fqn=%s error=%s",
                norm,
                exc,
            )
            not_found.append(norm)
            continue

        data["excluded"] = True
        write_json(catalog_path, data)
        marked.append(norm)
        logger.info(
            "event=exclude_marked component=exclude operation=run_exclude "
            "fqn=%s bucket=%s status=success",
            norm,
            bucket,
        )

    return ExcludeOutput(marked=marked, not_found=not_found)
