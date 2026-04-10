"""Init command output contracts."""

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class ScaffoldProjectOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    files_created: list[str]
    files_updated: list[str]
    files_skipped: list[str]


class ScaffoldHooksOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    hook_created: bool
    hooks_path_configured: bool
