"""FreeTDS installation and unixODBC registration helpers."""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

from shared.output_models.init import FreeTdsCheckOutput

logger = logging.getLogger(__name__)


def _run_command(command: list[str]) -> str:
    return subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _parse_odbcinst_j(output: str) -> Path | None:
    for line in output.splitlines():
        if line.startswith("DRIVERS"):
            _, _, value = line.partition(":")
            path = value.strip()
            if path:
                return Path(path)
    return None


def _find_driver_path(prefix: Path) -> Path | None:
    candidates = sorted((prefix / "lib").glob("libtdsodbc.*"))
    return candidates[0] if candidates else None


def _find_setup_path(prefix: Path) -> Path | None:
    candidates = sorted((prefix / "lib").glob("libtdsS.*"))
    return candidates[0] if candidates else None


def _register_driver(
    *,
    registration_path: Path,
    driver_path: Path,
    setup_path: Path | None,
) -> None:
    lines = [
        "[FreeTDS]",
        "Description=FreeTDS ODBC Driver",
        f"Driver={driver_path}",
    ]
    if setup_path is not None:
        lines.append(f"Setup={setup_path}")
    lines.append("UsageCount=1")

    existing = registration_path.read_text(encoding="utf-8") if registration_path.exists() else ""
    content = existing.rstrip()
    if content:
        content += "\n\n"
    content += "\n".join(lines) + "\n"
    registration_path.parent.mkdir(parents=True, exist_ok=True)
    registration_path.write_text(content, encoding="utf-8")


def _query_registration_file() -> Path | None:
    return _parse_odbcinst_j(_run_command(["odbcinst", "-j"]))


def _is_registered() -> bool:
    output = _run_command(["odbcinst", "-q", "-d"])
    drivers = {line.strip().strip("[]") for line in output.splitlines() if line.strip()}
    return "FreeTDS" in drivers


def run_check_freetds(register_missing: bool = False) -> FreeTdsCheckOutput:
    """Check Homebrew FreeTDS installation and unixODBC registration state."""
    if os.name == "nt":
        return FreeTdsCheckOutput(
            supported_platform=False,
            installed=False,
            unixodbc_present=False,
            registered=False,
            auto_registered=False,
            registration_file=None,
            driver_lib_path=None,
            message="Windows is not supported for /init-ad-migration SQL Server setup.",
        )

    try:
        _run_command(["brew", "list", "--formula", "freetds"])
    except (FileNotFoundError, subprocess.CalledProcessError):
        logger.info("event=freetds_check status=not_installed")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=False,
            unixodbc_present=False,
            registered=False,
            auto_registered=False,
            registration_file=None,
            driver_lib_path=None,
            message="FreeTDS is not installed.",
        )

    registration_file: Path | None = None
    try:
        registration_file = _query_registration_file()
        registered = _is_registered()
    except FileNotFoundError:
        logger.warning("event=freetds_check status=missing_odbcinst")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=True,
            unixodbc_present=False,
            registered=False,
            auto_registered=False,
            registration_file=None,
            driver_lib_path=None,
            message="odbcinst is not available. unixODBC is required for FreeTDS registration.",
        )

    if registered:
        logger.info("event=freetds_check status=registered")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=True,
            unixodbc_present=True,
            registered=True,
            auto_registered=False,
            registration_file=str(registration_file) if registration_file else None,
            driver_lib_path=None,
            message=None,
        )

    if not register_missing:
        logger.info("event=freetds_check status=unregistered")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=True,
            unixodbc_present=True,
            registered=False,
            auto_registered=False,
            registration_file=str(registration_file) if registration_file else None,
            driver_lib_path=None,
            message="FreeTDS is installed but not registered in unixODBC.",
        )

    if registration_file is None:
        raise RuntimeError("odbcinst -j did not report an odbcinst.ini path.")

    prefix = Path(_run_command(["brew", "--prefix", "freetds"]).strip())
    driver_path = _find_driver_path(prefix)
    if driver_path is None:
        raise RuntimeError(f"FreeTDS driver library not found under {prefix / 'lib'}.")
    setup_path = _find_setup_path(prefix)

    _register_driver(
        registration_path=registration_file,
        driver_path=driver_path,
        setup_path=setup_path,
    )

    if not _is_registered():
        raise RuntimeError("FreeTDS registration did not appear in odbcinst after update.")

    logger.info(
        "event=freetds_check status=registered_after_repair registration_file=%s",
        registration_file,
    )
    return FreeTdsCheckOutput(
        supported_platform=True,
        installed=True,
        unixodbc_present=True,
        registered=True,
        auto_registered=True,
        registration_file=str(registration_file),
        driver_lib_path=str(driver_path),
        message=None,
    )
