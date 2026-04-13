from __future__ import annotations

import pytest

from tests.helpers import require_oracle_extract_env


@pytest.fixture
def oracle_extract_env() -> None:
    require_oracle_extract_env()
