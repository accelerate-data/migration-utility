"""Tests for shared.freetds and init check-freetds CLI."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from typer.testing import CliRunner

import shared.freetds as freetds
from shared.init import app


def test_check_freetds_unsupported_on_windows(monkeypatch) -> None:
    monkeypatch.setattr(freetds.os, "name", "nt", raising=False)

    result = freetds.run_check_freetds()

    assert result.supported_platform is False
    assert result.installed is False
    assert result.unixodbc_present is False
    assert result.registered is False
    assert "Windows" in (result.message or "")


def test_check_freetds_reports_missing_install(monkeypatch) -> None:
    def fake_run(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ["brew", "list", "--formula", "freetds"])

    monkeypatch.setattr(freetds, "_run_command", fake_run)
    monkeypatch.setattr(freetds.os, "name", "posix", raising=False)

    result = freetds.run_check_freetds()

    assert result.supported_platform is True
    assert result.installed is False
    assert result.registered is False
    assert result.auto_registered is False


def test_check_freetds_reports_missing_odbcinst(monkeypatch) -> None:
    def fake_run(command: list[str]) -> str:
        if command[:3] == ["brew", "list", "--formula"]:
            return "freetds\n"
        raise FileNotFoundError("odbcinst")

    monkeypatch.setattr(freetds, "_run_command", fake_run)
    monkeypatch.setattr(freetds.os, "name", "posix", raising=False)

    result = freetds.run_check_freetds()

    assert result.installed is True
    assert result.unixodbc_present is False
    assert result.registered is False
    assert "odbcinst" in (result.message or "")


def test_check_freetds_returns_registered_when_driver_exists(monkeypatch) -> None:
    def fake_run(command: list[str]) -> str:
        if command[:3] == ["brew", "list", "--formula"]:
            return "freetds\n"
        if command == ["odbcinst", "-q", "-d"]:
            return "[FreeTDS]\n"
        if command == ["odbcinst", "-j"]:
            return "DRIVERS............: /opt/homebrew/etc/odbcinst.ini\n"
        raise AssertionError(command)

    monkeypatch.setattr(freetds, "_run_command", fake_run)
    monkeypatch.setattr(freetds.os, "name", "posix", raising=False)

    result = freetds.run_check_freetds()

    assert result.installed is True
    assert result.unixodbc_present is True
    assert result.registered is True
    assert result.auto_registered is False
    assert result.registration_file == "/opt/homebrew/etc/odbcinst.ini"


def test_check_freetds_registers_missing_driver(monkeypatch, tmp_path: Path) -> None:
    registration_file = tmp_path / "odbcinst.ini"
    lib_dir = tmp_path / "lib"
    lib_dir.mkdir()
    driver_lib = lib_dir / "libtdsodbc.so"
    driver_lib.write_text("", encoding="utf-8")
    setup_lib = lib_dir / "libtdsS.so"
    setup_lib.write_text("", encoding="utf-8")
    queried = {"registered": False}

    def fake_run(command: list[str]) -> str:
        if command[:3] == ["brew", "list", "--formula"]:
            return "freetds\n"
        if command == ["odbcinst", "-q", "-d"]:
            return "[FreeTDS]\n" if queried["registered"] else "[OtherDriver]\n"
        if command == ["odbcinst", "-j"]:
            return f"DRIVERS............: {registration_file}\n"
        if command == ["brew", "--prefix", "freetds"]:
            return str(tmp_path)
        raise AssertionError(command)

    def fake_register(*, registration_path: Path, driver_path: Path, setup_path: Path | None) -> None:
        queried["registered"] = True
        assert registration_path == registration_file
        assert driver_path == driver_lib
        assert setup_path == setup_lib

    monkeypatch.setattr(freetds, "_run_command", fake_run)
    monkeypatch.setattr(freetds, "_register_driver", fake_register)
    monkeypatch.setattr(freetds.os, "name", "posix", raising=False)

    result = freetds.run_check_freetds(register_missing=True)

    assert result.registered is True
    assert result.auto_registered is True
    assert result.driver_lib_path == str(driver_lib)
    assert result.registration_file == str(registration_file)


def test_check_freetds_cli_emits_json(monkeypatch) -> None:
    monkeypatch.setattr(
        "shared.init.run_check_freetds",
        lambda register_missing=False: freetds.FreeTdsCheckOutput(
            supported_platform=True,
            installed=True,
            unixodbc_present=True,
            registered=True,
            auto_registered=register_missing,
            registration_file="/tmp/odbcinst.ini",
            driver_lib_path="/tmp/libtdsodbc.so",
            message=None,
        ),
    )

    runner = CliRunner()
    result = runner.invoke(app, ["check-freetds", "--register-missing"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["registered"] is True
    assert payload["auto_registered"] is True
