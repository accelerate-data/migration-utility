from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared import dry_run
from shared import dry_run_core
from shared import generate_sources as gen_src
from shared.dry_run_support import status as dry_run_status
from shared.output_models.dry_run import DryRunOutput
from tests.unit.dry_run.dry_run_test_helpers import (
    _add_source_table,
    _add_table_to_project,
    _cli_runner,
    _make_bare_project,
    _make_exclude_project,
    _make_project,
    _make_reset_project,
)

def test_dry_run_core_is_split_into_support_modules() -> None:
    """dry_run_core stays as a compatibility barrel over focused support modules."""
    from shared.dry_run_support import excluded_warnings, exclusions, readiness, reset, status

    assert dry_run_core.run_ready is readiness.run_ready
    assert dry_run_core.run_exclude is exclusions.run_exclude
    assert dry_run_core.run_reset_migration is reset.run_reset_migration
    assert dry_run_core.run_sync_excluded_warnings is excluded_warnings.run_sync_excluded_warnings
    assert status.run_status is not None
