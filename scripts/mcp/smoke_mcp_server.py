#!/usr/bin/env python3
"""Standalone stdio MCP smoke tester for local developer use.

Checks:
1) server process starts
2) initialize succeeds
3) tools/list succeeds
4) optional tools/call sequence succeeds
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
def _json_line(proc: subprocess.Popen[str], timeout_sec: float, method: str) -> dict[str, Any]:
    assert proc.stdout is not None
    deadline = time.time() + timeout_sec
    non_json_lines: list[str] = []
    while True:
        if time.time() > deadline:
            raise RuntimeError(f"timeout waiting for response to {method}")

        line = proc.stdout.readline()
        if not line:
            stderr_text = ""
            if proc.stderr is not None:
                try:
                    stderr_text = proc.stderr.read().strip()
                except Exception:
                    stderr_text = ""
            stdout_hint = ""
            if non_json_lines:
                stdout_hint = f"; stdout_non_json={' | '.join(non_json_lines[:5])}"
            detail = f"; stderr={stderr_text}" if stderr_text else ""
            raise RuntimeError(f"server exited while waiting for {method}{detail}{stdout_hint}")

        line = line.strip()
        if not line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            non_json_lines.append(line[:200])
            continue


class StdioJsonRpc:
    def __init__(self, proc: subprocess.Popen[str], timeout_sec: float) -> None:
        self.proc = proc
        self.timeout_sec = timeout_sec
        self.request_id = 1

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        rid = self.request_id
        self.request_id += 1

        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            payload["params"] = params

        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

        while True:
            msg = _json_line(self.proc, self.timeout_sec, method)
            if msg.get("id") != rid:
                continue
            if "error" in msg:
                raise RuntimeError(f"{method} returned error: {json.dumps(msg['error'])}")
            return msg

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()


def parse_tool_calls(raw: str) -> list[dict[str, Any]]:
    if raw.strip() == "":
        return []
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError("--tool-calls-json must be a JSON array")

    calls: list[dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            raise ValueError("each tool call item must be an object")
        name = item.get("name")
        args = item.get("arguments", {})
        if not isinstance(name, str) or name == "":
            raise ValueError("each tool call requires non-empty string field 'name'")
        if not isinstance(args, dict):
            raise ValueError("tool call field 'arguments' must be an object")
        calls.append({"name": name, "arguments": args})
    return calls


def main() -> int:
    parser = argparse.ArgumentParser(description="Standalone MCP smoke test")
    parser.add_argument("--server-cmd", required=True, help="shell command to launch server")
    parser.add_argument("--timeout-sec", type=float, default=20.0)
    parser.add_argument("--tool-calls-json", default="[]")
    args = parser.parse_args()

    tool_calls = parse_tool_calls(args.tool_calls_json)

    try:
        proc = subprocess.Popen(
            shlex.split(args.server_cmd),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"failed to launch MCP command: {args.server_cmd} ({exc})"
        ) from exc

    client = StdioJsonRpc(proc, timeout_sec=args.timeout_sec)

    try:
        init_resp = client.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "local-mcp-smoke", "version": "0.1.0"},
            },
        )
        server_info = (init_resp.get("result") or {}).get("serverInfo", {})
        print(
            "[ok] initialize "
            f"server={server_info.get('name', 'unknown')} version={server_info.get('version', 'unknown')}"
        )

        client.notify("notifications/initialized")
        print("[ok] notifications/initialized")

        tools_resp = client.request("tools/list")
        tools = ((tools_resp.get("result") or {}).get("tools") or [])
        if not isinstance(tools, list):
            raise RuntimeError("tools/list result did not contain a tools array")

        tool_names = [t.get("name") for t in tools if isinstance(t, dict) and isinstance(t.get("name"), str)]
        print(f"[ok] tools/list count={len(tool_names)} tools={tool_names}")

        for tool in tool_calls:
            name = tool["name"]
            if name not in tool_names:
                raise RuntimeError(f"required tool '{name}' not exposed. available={tool_names}")
            resp = client.request("tools/call", {"name": name, "arguments": tool["arguments"]})
            result = resp.get("result")
            if isinstance(result, dict) and result.get("isError") is True:
                raise RuntimeError(f"tool {name} returned isError=true: {json.dumps(result)}")
            print(f"[ok] tools/call {name}")

        return 0
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
