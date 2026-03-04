#!/usr/bin/env python3
"""Read an env key from mcpServers.<server>.env in settings.local."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: read_mcp_env.py <server-key> <env-key>", file=sys.stderr)
        return 2

    server_key = sys.argv[1]
    env_key = sys.argv[2]

    candidates = [
        Path('.claude/settings.local'),
        Path('agent-sources/workspace/.claude/settings.local'),
    ]

    settings_path = next((p for p in candidates if p.is_file()), None)
    if settings_path is None:
        print("", end="")
        return 0

    try:
        data = json.loads(settings_path.read_text(encoding='utf-8'))
    except Exception:
        print("", end="")
        return 0

    env = (((data.get('mcpServers') or {}).get(server_key) or {}).get('env') or {})
    value = env.get(env_key)
    if isinstance(value, str):
        print(value)
    else:
        print("", end="")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
