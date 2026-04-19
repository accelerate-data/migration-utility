from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

_TESTS_DIR = Path(__file__).parent
_PROFILE_FIXTURES = _TESTS_DIR / "fixtures"

_VALID_VIEW_PROFILE = {
    "classification": "stg",
    "rationale": "Single-source pass-through.",
    "source": "llm",
}


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy profile fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "profile"
    shutil.copytree(_PROFILE_FIXTURES, dst)
    return tmp, dst
