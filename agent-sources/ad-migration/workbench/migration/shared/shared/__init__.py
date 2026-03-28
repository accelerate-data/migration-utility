"""Shared library for migration skills.

Public re-exports so callers can do:
    from shared import ir, loader, dialect, name_resolver
"""

from shared import dialect, ir, loader, name_resolver

__all__ = ["dialect", "ir", "loader", "name_resolver"]
