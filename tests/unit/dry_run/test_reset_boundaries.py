"""Boundary tests for dry-run reset split modules and facade."""

from __future__ import annotations


def test_reset_facade_exports_public_dispatcher_and_legacy_helpers() -> None:
    from shared.dry_run_support import reset

    assert callable(reset.run_reset_migration)
    assert callable(reset.delete_if_present)
    assert callable(reset.delete_tree_if_present)
    assert callable(reset.reset_table_sections)
    assert callable(reset.reset_writer_refactor)
    assert callable(reset.prepare_reset_migration_all_manifest)
    assert callable(reset.run_reset_migration_all)


def test_reset_support_modules_export_owned_entrypoints() -> None:
    from shared.dry_run_support.reset_files import delete_if_present, delete_tree_if_present
    from shared.dry_run_support.reset_global import (
        prepare_reset_migration_all_manifest,
        run_reset_migration_all,
    )
    from shared.dry_run_support.reset_stage import (
        reset_table_sections,
        reset_writer_refactor,
        run_reset_migration_stage,
    )

    assert callable(delete_if_present)
    assert callable(delete_tree_if_present)
    assert callable(prepare_reset_migration_all_manifest)
    assert callable(run_reset_migration_all)
    assert callable(reset_table_sections)
    assert callable(reset_writer_refactor)
    assert callable(run_reset_migration_stage)
