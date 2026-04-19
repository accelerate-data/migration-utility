from __future__ import annotations

from pathlib import Path

from shared.init_support.local_env import run_write_local_env_overrides, write_local_env_overrides


def test_write_local_env_overrides_updates_existing_values(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text('KEEP_ME="1"\nLOCAL_TOOL_PATH="/old"\n', encoding="utf-8")

    result = run_write_local_env_overrides(tmp_path, {"LOCAL_TOOL_PATH": '/opt/"tool"'})

    assert result.changed is True
    assert env_path.read_text(encoding="utf-8") == 'KEEP_ME="1"\nLOCAL_TOOL_PATH="/opt/\\"tool\\""\n'


def test_write_local_env_overrides_bool_wrapper(tmp_path: Path) -> None:
    assert write_local_env_overrides(tmp_path, {"LOCAL_TOOL_PATH": "/opt/tool"}) is True
    assert write_local_env_overrides(tmp_path, {"LOCAL_TOOL_PATH": "/opt/tool"}) is False
