"""Symmetric diff helpers for refactor."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _row_to_key(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """Convert a row dict to a hashable key for multiset comparison."""
    return tuple(sorted((key, str(value)) for key, value in row.items()))


def symmetric_diff(rows_a: list[dict[str, Any]], rows_b: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute the symmetric difference of two row-dict lists."""
    keys_a = [_row_to_key(row) for row in rows_a]
    keys_b = [_row_to_key(row) for row in rows_b]

    counter_a = Counter(keys_a)
    counter_b = Counter(keys_b)
    a_minus_b_counter = counter_a - counter_b
    b_minus_a_counter = counter_b - counter_a

    def _keys_to_rows(counter: Counter[tuple[tuple[str, str], ...]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, count in counter.items():
            row = dict(key)
            for _ in range(count):
                rows.append(row)
        return rows

    a_minus_b = _keys_to_rows(a_minus_b_counter)
    b_minus_a = _keys_to_rows(b_minus_a_counter)
    return {
        "equivalent": len(a_minus_b) == 0 and len(b_minus_a) == 0,
        "a_minus_b": a_minus_b,
        "b_minus_a": b_minus_a,
        "a_count": len(rows_a),
        "b_count": len(rows_b),
    }
