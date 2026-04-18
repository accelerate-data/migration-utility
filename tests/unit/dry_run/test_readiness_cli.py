from __future__ import annotations

import json

from shared import dry_run
from tests.unit.dry_run.dry_run_test_helpers import (
    _cli_runner,
    _make_project,
)


def test_cli_ready_scope() -> None:
    """CLI ready returns JSON for object-scoped readiness."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "scope", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output["object"]["ready"] is True
        assert output["project"]["reason"] == "ok"
        assert output["object"]["reason"] == "ok"


def test_cli_ready_invalid_stage() -> None:
    """CLI ready with invalid stage still returns JSON (ready=False)."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "bogus", "--object", "silver.DimCustomer", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["reason"] == "invalid_stage"


def test_cli_ready_project_only() -> None:
    """CLI ready supports project-only readiness without object input."""
    tmp, root = _make_project()
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "setup-ddl", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is True
        assert output["project"]["ready"] is True
        assert output.get("object") is None
        assert output["project"]["reason"] == "ok"


def test_cli_ready_test_gen_missing_target_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear target setup code."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"


def test_cli_ready_test_gen_missing_sandbox_exits_with_code() -> None:
    """CLI ready test-gen exits non-zero with a clear sandbox setup code."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "test-gen", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"


def test_cli_ready_generate_missing_target_preserves_zero_exit() -> None:
    """CLI ready generate keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_target=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "generate", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "TARGET_NOT_CONFIGURED"


def test_cli_ready_refactor_missing_sandbox_preserves_zero_exit() -> None:
    """CLI ready refactor keeps JSON-only readiness behavior for setup failures."""
    tmp, root = _make_project(include_sandbox=False)
    with tmp:
        result = _cli_runner.invoke(
            dry_run.app,
            ["ready", "refactor", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["ready"] is False
        assert output["project"]["code"] == "SANDBOX_NOT_CONFIGURED"
