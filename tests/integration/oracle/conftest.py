from __future__ import annotations

import pytest

from tests.helpers import configure_oracle_extract_env, require_oracle_extract_env


@pytest.fixture
def oracle_extract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_oracle_extract_env(monkeypatch)
    require_oracle_extract_env()
