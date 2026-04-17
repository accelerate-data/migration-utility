"""FreeTDS installation and unixODBC registration helpers."""

from __future__ import annotations

import logging
import os  # noqa: F401 - compatibility export for tests and older callers
import shutil
import subprocess
from pathlib import Path

from shared.output_models.init import FreeTdsCheckOutput
from shared.platform import classify_host_platform

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


def _library_search_dirs(prefix: Path) -> list[Path]:
    lib_dir = prefix / "lib"
    lib64_dir = prefix / "lib64"
    return [
        lib_dir,
        lib_dir / "odbc",
        *sorted(lib_dir.glob("*-linux-gnu")),
        *sorted(lib_dir.glob("*-linux-gnu/odbc")),
        lib64_dir,
        lib64_dir / "odbc",
    ]


def _find_library_path(prefix: Path, pattern: str) -> Path | None:
    candidates = sorted(
        candidate
        for directory in _library_search_dirs(prefix)
        for candidate in directory.glob(pattern)
    )
    return candidates[0] if candidates else None


def _find_driver_path(prefix: Path) -> Path | None:
    return _find_library_path(prefix, "libtdsodbc.*")


def _find_setup_path(prefix: Path) -> Path | None:
    return _find_library_path(prefix, "libtdsS.*")


def _classify_platform_slug() -> str:
    return classify_host_platform().slug


def _command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def _freetds_installed(platform_slug: str) -> bool:
    if platform_slug == "macos":
        try:
            _run_command(["brew", "list", "--formula", "freetds"])
            return True
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False
    return _command_exists("tsql")


def _resolve_freetds_prefix(platform_slug: str) -> Path | None:
    if platform_slug == "macos":
        try:
            return Path(_run_command(["brew", "--prefix", "freetds"]).strip())
        except (FileNotFoundError, subprocess.CalledProcessError):
            return None

    for candidate in (Path("/usr"), Path("/usr/local"), Path("/opt/homebrew"), Path("/opt/local")):
        if _find_driver_path(candidate) is not None:
            return candidate
    return None


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
    """Check FreeTDS installation and unixODBC registration state."""
    platform_slug = _classify_platform_slug()
    if platform_slug == "windows":
        return FreeTdsCheckOutput(
            supported_platform=False,
            installed=False,
            unixodbc_present=False,
            registered=False,
            auto_registered=False,
            registration_file=None,
            driver_lib_path=None,
            message="Native Windows is not supported for /init-ad-migration SQL Server setup. Use WSL.",
        )

    if not _freetds_installed(platform_slug):
        logger.info("event=freetds_check status=not_installed")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=False,
            unixodbc_present=False,
            registered=False,
            auto_registered=False,
            registration_file=None,
            driver_lib_path=None,
            message=(
                "FreeTDS is not installed. Install FreeTDS and unixODBC using "
                "your platform package manager."
            ),
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

    prefix = _resolve_freetds_prefix(platform_slug)
    if prefix is None:
        logger.warning("event=freetds_check status=unregistered_auto_register_unavailable")
        return FreeTdsCheckOutput(
            supported_platform=True,
            installed=True,
            unixodbc_present=True,
            registered=False,
            auto_registered=False,
            registration_file=str(registration_file),
            driver_lib_path=None,
            message=(
                "FreeTDS is installed but not registered in unixODBC; automatic "
                "registration is only supported when the FreeTDS library path can be resolved."
            ),
        )

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
