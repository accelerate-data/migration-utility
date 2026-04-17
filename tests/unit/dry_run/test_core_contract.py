from __future__ import annotations



from shared import dry_run_core

def test_dry_run_core_is_split_into_support_modules() -> None:
    """dry_run_core stays as a compatibility barrel over focused support modules."""
    from shared.dry_run_support import excluded_warnings, exclusions, readiness, reset, status

    assert dry_run_core.run_ready is readiness.run_ready
    assert dry_run_core.run_exclude is exclusions.run_exclude
    assert dry_run_core.run_reset_migration is reset.run_reset_migration
    assert dry_run_core.run_sync_excluded_warnings is excluded_warnings.run_sync_excluded_warnings
    assert status.run_status is not None
