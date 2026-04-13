"""Make server.py importable as `import server` from test context."""

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_SERVER_DIR = _TESTS_DIR.parent
for path in (_SERVER_DIR, _TESTS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
