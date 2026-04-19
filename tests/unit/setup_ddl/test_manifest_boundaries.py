"""Import-boundary tests for split setup-ddl manifest support modules."""

from __future__ import annotations


def test_manifest_facade_reexports_existing_public_entrypoints() -> None:
    from shared.setup_ddl_support import manifest

    assert callable(manifest.require_technology)
    assert callable(manifest.read_manifest_strict)
    assert callable(manifest.read_manifest_or_empty)
    assert callable(manifest.run_write_partial_manifest)
    assert callable(manifest.run_read_handoff)
    assert callable(manifest.run_write_manifest)
    assert callable(manifest.get_connection_identity)
    assert callable(manifest.identity_changed)
    assert callable(manifest.build_runtime_role)
    assert callable(manifest.build_oracle_schema_summary)
    assert isinstance(manifest.TECH_DIALECT, dict)


def test_manifest_support_modules_own_split_entrypoints() -> None:
    from shared.setup_ddl_support.manifest_io import (
        read_manifest_or_empty,
        read_manifest_strict,
        require_technology,
        run_read_handoff,
        run_write_manifest,
        run_write_partial_manifest,
    )
    from shared.setup_ddl_support.oracle_schema_summary import build_oracle_schema_summary
    from shared.setup_ddl_support.runtime_identity import (
        build_runtime_role,
        get_connection_identity,
        identity_changed,
    )

    assert callable(require_technology)
    assert callable(read_manifest_strict)
    assert callable(read_manifest_or_empty)
    assert callable(run_write_partial_manifest)
    assert callable(run_read_handoff)
    assert callable(run_write_manifest)
    assert callable(build_oracle_schema_summary)
    assert callable(get_connection_identity)
    assert callable(identity_changed)
    assert callable(build_runtime_role)
