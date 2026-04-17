"""Init command output contracts."""

from pydantic import BaseModel, Field

from shared.output_models.shared import OUTPUT_CONFIG


class ScaffoldProjectOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    files_created: list[str]
    files_updated: list[str]
    files_skipped: list[str]
    written_paths: list[str] = Field(default_factory=list)


class ScaffoldHooksOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    hook_created: bool
    hooks_path_configured: bool
    written_paths: list[str] = Field(default_factory=list)


class FreeTdsCheckOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    supported_platform: bool
    installed: bool
    unixodbc_present: bool
    registered: bool
    auto_registered: bool
    registration_file: str | None
    driver_lib_path: str | None
    message: str | None


class LocalEnvOverrideWriteOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    file: str
    changed: bool
