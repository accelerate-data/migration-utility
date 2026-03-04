#!/usr/bin/env python3
"""Resolve an MCP server launch command from settings.local."""

from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: read_mcp_cmd.py <server-key>", file=sys.stderr)
        return 2

    server_key = sys.argv[1]
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

    server = ((data.get('mcpServers') or {}).get(server_key) or {})
    command = server.get('command')
    args = server.get('args') or []

    if not isinstance(command, str) or not command.strip():
        print("", end="")
        return 0
    if not isinstance(args, list) or any(not isinstance(a, str) for a in args):
        args = []

    print(shlex.join([command, *args]))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
