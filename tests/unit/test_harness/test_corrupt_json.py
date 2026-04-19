from __future__ import annotations

import json
from pathlib import Path




class TestCorruptJsonHandling:
    """Verify CLI commands handle corrupt JSON inputs gracefully."""

    def test_sandbox_up_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-up with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_sandbox_status_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-status with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        # sandbox-status reads runtime.sandbox from manifest, which will fail
        result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_execute_spec_corrupt_json_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with corrupt test-spec JSON exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "corrupt-spec.json"
        spec.write_text("{not valid json", encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "SBX_ABC123000000"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_execute_spec_missing_required_fields_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with valid JSON but missing unit_tests exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "empty-spec.json"
        spec.write_text('{"model": "stg_test"}', encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "SBX_ABC123000000"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1
