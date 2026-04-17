"""Shared helpers for test_harness unit tests."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from shared.sandbox.sql_server import SqlServerSandbox


def _make_backend() -> SqlServerSandbox:
    return SqlServerSandbox(
        host="localhost",
        port="1433",
        password="TestPass123",
    )


def _mock_connect_factory(
    *,
    source_cursor: MagicMock | None = None,
    sandbox_cursor: MagicMock | None = None,
    default_cursor: MagicMock | None = None,
) -> Callable[..., Any]:
    """Return a _connect side_effect that routes by database keyword arg.

    Routing:
    - database starts with 'SBX_' → sandbox_cursor (if set)
    - named non-sandbox database → source_cursor (if set)
    - database=None (source default) → default_cursor if set, else noop cursor
    - fallback → noop cursor that returns None for fetchone (not a view)

    The noop fallback keeps existing tests that don't configure every cursor
    from breaking when _ensure_view_tables opens a source connection.
    """
    @contextmanager
    def _fake_connect(*, database: str | None = None):
        conn = MagicMock()
        if database and database.startswith("SBX_") and sandbox_cursor is not None:
            conn.cursor.return_value = sandbox_cursor
        elif source_cursor is not None and database and not database.startswith("SBX_"):
            conn.cursor.return_value = source_cursor
        elif default_cursor is not None:
            conn.cursor.return_value = default_cursor
        else:
            noop = MagicMock()
            noop.fetchone.return_value = None  # legacy "not a view" for _ensure_view_tables
            noop.fetchall.return_value = []    # "not a view" for _ensure_view_tables (fetchall path)
            conn.cursor.return_value = noop
        yield conn
    return _fake_connect
