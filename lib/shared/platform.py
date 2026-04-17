"""Host platform classification helpers for local init workflows."""

from __future__ import annotations

import platform
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HostPlatform:
    """Host OS classification used by init prerequisite decisions."""

    slug: str
    supported: bool
    display_name: str


def _read_proc_version_text() -> str:
    try:
        return Path("/proc/version").read_text(encoding="utf-8")
    except OSError:
        return ""


def _read_osrelease_text() -> str:
    try:
        return Path("/etc/os-release").read_text(encoding="utf-8")
    except OSError:
        return ""


def classify_host_platform() -> HostPlatform:
    """Classify the local host for init install and prerequisite guidance."""
    system = platform.system()
    if system == "Darwin":
        return HostPlatform(slug="macos", supported=True, display_name="macOS")
    if system == "Windows":
        return HostPlatform(slug="windows", supported=False, display_name="Windows")
    if system == "Linux":
        release_text = f"{_read_osrelease_text()}\n{_read_proc_version_text()}".lower()
        if "microsoft" in release_text or "wsl" in release_text:
            return HostPlatform(slug="wsl", supported=True, display_name="WSL")
        return HostPlatform(slug="linux", supported=True, display_name="Linux")
    return HostPlatform(
        slug="unsupported",
        supported=False,
        display_name=system or "Unknown",
    )


def supports_homebrew_install(platform_info: HostPlatform) -> bool:
    """Return true when init may use the macOS Homebrew install path."""
    return platform_info.slug == "macos"


def supports_native_windows(platform_info: HostPlatform) -> bool:
    """Return true when the host is native Windows rather than WSL."""
    return platform_info.slug == "windows"


def build_init_platform_gate_message(platform_info: HostPlatform | None = None) -> str:
    """Build the user-facing stop message for unsupported init hosts."""
    platform_info = platform_info or classify_host_platform()
    if supports_native_windows(platform_info):
        return (
            "Native Windows is not supported for /init-ad-migration local execution. "
            "Use WSL, macOS, or Linux for the local workflow."
        )
    return (
        f"{platform_info.display_name} is not supported for /init-ad-migration local execution. "
        "Use macOS, Linux, or WSL."
    )
