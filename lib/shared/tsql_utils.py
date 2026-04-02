"""Shared T-SQL text utilities."""

from __future__ import annotations


def mask_tsql(sql: str) -> str:
    """Mask strings, comments, and bracketed identifiers while preserving indices."""
    chars = list(sql)
    i = 0
    while i < len(chars):
        ch = chars[i]
        nxt = chars[i + 1] if i + 1 < len(chars) else ""

        if ch == "'":
            chars[i] = " "
            i += 1
            while i < len(chars):
                if chars[i] == "'":
                    chars[i] = " "
                    if i + 1 < len(chars) and chars[i + 1] == "'":
                        chars[i + 1] = " "
                        i += 2
                        continue
                    i += 1
                    break
                chars[i] = " "
                i += 1
            continue

        if ch == "[":
            chars[i] = " "
            i += 1
            while i < len(chars):
                chars[i] = " "
                if sql[i] == "]":
                    i += 1
                    break
                i += 1
            continue

        if ch == "-" and nxt == "-":
            chars[i] = chars[i + 1] = " "
            i += 2
            while i < len(chars) and chars[i] != "\n":
                chars[i] = " "
                i += 1
            continue

        if ch == "/" and nxt == "*":
            chars[i] = chars[i + 1] = " "
            i += 2
            while i < len(chars):
                chars[i] = " "
                if i + 1 < len(chars) and sql[i] == "*" and sql[i + 1] == "/":
                    chars[i + 1] = " "
                    i += 2
                    break
                i += 1
            continue

        i += 1

    return "".join(chars)
