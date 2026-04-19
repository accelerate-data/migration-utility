"""Local environment override writing for migration project init."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from shared.output_models.init import LocalEnvOverrideWriteOutput

logger = logging.getLogger(__name__)

ENV_ASSIGNMENT_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=")


def quote_env_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def run_write_local_env_overrides(
    project_root: Path,
    overrides: dict[str, str],
) -> LocalEnvOverrideWriteOutput:
    """Write machine-local, non-secret overrides into project_root/.env."""
    project_root.mkdir(parents=True, exist_ok=True)
    env_path = project_root / ".env"
    existing_lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    updated_lines = list(existing_lines)
    changed = False

    for key, value in overrides.items():
        rendered = f"{key}={quote_env_value(value)}"
        replacement_index: int | None = None
        for idx, line in enumerate(updated_lines):
            match = ENV_ASSIGNMENT_RE.match(line)
            if match and match.group(1) == key:
                replacement_index = idx
                break
        if replacement_index is None:
            updated_lines.append(rendered)
            changed = True
        elif updated_lines[replacement_index] != rendered:
            updated_lines[replacement_index] = rendered
            changed = True

    if not changed:
        logger.info(
            "event=local_env_override_write status=skipped file=%s",
            env_path,
        )
        return LocalEnvOverrideWriteOutput(file=str(env_path), changed=False)

    env_path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
    logger.info(
        "event=local_env_override_write status=updated file=%s override_count=%d",
        env_path,
        len(overrides),
    )
    return LocalEnvOverrideWriteOutput(file=str(env_path), changed=True)


def write_local_env_overrides(project_root: Path, overrides: dict[str, str]) -> bool:
    """Backwards-compatible bool wrapper for tests and callers."""
    return run_write_local_env_overrides(project_root, overrides).changed
