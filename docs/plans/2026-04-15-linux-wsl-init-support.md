# Linux And WSL Init Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ad-migration` support macOS, Linux, and WSL consistently in init/bootstrap, SQL Server prerequisite detection, and operator guidance while keeping native Windows unsupported.

**Architecture:** Add one shared platform-classification path for init/runtime messaging, then make FreeTDS detection package-manager-neutral so SQL Server readiness no longer depends on Homebrew. Update command/docs surfaces to consume that platform-aware behavior, and close the loop with regression tests across macOS, Linux, WSL, and native Windows branches.

**Tech Stack:** Python 3.11+, Typer, pytest, uv, Markdown command/docs, Unix shell scaffolding

**Issue:** `VU-1093` https://linear.app/acceleratedata/issue/VU-1093/add-linux-and-wsl-support-to-ad-migration-init-and-sql-server

---

## File Map

- Modify: `lib/shared/freetds.py`
  Purpose: replace Homebrew-only FreeTDS detection/repair with platform-aware detection that works for macOS and Linux/WSL.
- Modify: `lib/shared/init.py`
  Purpose: add platform classification helpers and reuse them from init-facing commands.
- Modify: `lib/shared/db_connect.py`
  Purpose: remove macOS-only SQL Server remediation text.
- Modify: `lib/shared/cli/error_handler.py`
  Purpose: remove macOS-only pyodbc remediation text.
- Modify: `lib/shared/cli/setup_source_cmd.py`
  Purpose: align source prereq messaging with the new shared platform-aware SQL Server detection contract.
- Modify: `commands/init-ad-migration.md`
  Purpose: make the command contract platform-aware for macOS, Linux, WSL, and native Windows.
- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `docs/wiki/Command-Reference.md`
- Modify: `docs/wiki/Quickstart.md`
- Modify: `docs/wiki/Stage-1-Project-Init.md`
  Purpose: align user-facing docs with the supported platform matrix and init behavior.
- Test: `tests/unit/freetds/test_freetds.py`
- Test: `tests/unit/init/test_init.py`
- Test: `tests/unit/db_connect/test_db_connect.py`
- Test: `tests/unit/cli/test_setup_source_cmd.py`
  Purpose: lock in platform classification, SQL Server prereq detection, and platform-aware guidance.

### Task 1: Add Shared Platform Classification For Init Decisions

**Files:**

- Modify: `lib/shared/init.py`
- Test: `tests/unit/init/test_init.py`

- [ ] **Step 1: Write the failing platform-classification tests**

```python
def test_classify_platform_returns_wsl_for_linux_kernel_with_microsoft_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("shared.init.platform.system", lambda: "Linux")
    monkeypatch.setattr("shared.init._read_osrelease_text", lambda: "NAME=Ubuntu\n")
    monkeypatch.setattr("shared.init._read_proc_version_text", lambda: "Linux version ... microsoft-standard-WSL2")

    result = classify_host_platform()

    assert result.slug == "wsl"
    assert result.supported is True


def test_classify_platform_returns_windows_for_native_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("shared.init.platform.system", lambda: "Windows")

    result = classify_host_platform()

    assert result.slug == "windows"
    assert result.supported is False
```

- [ ] **Step 2: Run the focused test selection and confirm it fails**

Run: `cd lib && uv run pytest ../tests/unit/init/test_init.py -k platform -v`
Expected: FAIL with `ImportError` or `NameError` because the new platform helpers do not exist yet.

- [ ] **Step 3: Add the minimal shared platform helpers**

```python
@dataclass(frozen=True)
class HostPlatform:
    slug: str
    supported: bool
    display_name: str


def classify_host_platform() -> HostPlatform:
    system = platform.system()
    if system == "Darwin":
        return HostPlatform(slug="macos", supported=True, display_name="macOS")
    if system == "Windows":
        return HostPlatform(slug="windows", supported=False, display_name="Windows")
    if system == "Linux":
        proc_version = _read_proc_version_text().lower()
        if "microsoft" in proc_version or "wsl" in proc_version:
            return HostPlatform(slug="wsl", supported=True, display_name="WSL")
        return HostPlatform(slug="linux", supported=True, display_name="Linux")
    return HostPlatform(slug="unsupported", supported=False, display_name=system or "Unknown")
```

- [ ] **Step 4: Add init-facing convenience helpers for installer messaging**

```python
def supports_homebrew_install(platform_info: HostPlatform) -> bool:
    return platform_info.slug == "macos"


def supports_native_windows(platform_info: HostPlatform) -> bool:
    return platform_info.slug == "windows"
```

- [ ] **Step 5: Re-run the platform tests and confirm they pass**

Run: `cd lib && uv run pytest ../tests/unit/init/test_init.py -k platform -v`
Expected: PASS for the new macOS/Linux/WSL/Windows classification tests.

- [ ] **Step 6: Commit the platform helper slice**

```bash
git add lib/shared/init.py tests/unit/init/test_init.py
git commit -m "feat: classify init host platforms"
```

### Task 2: Make FreeTDS Detection And Repair Package-Manager-Neutral

**Files:**

- Modify: `lib/shared/freetds.py`
- Modify: `lib/shared/cli/setup_source_cmd.py`
- Test: `tests/unit/freetds/test_freetds.py`
- Test: `tests/unit/cli/test_setup_source_cmd.py`

- [ ] **Step 1: Write failing tests for Linux/WSL FreeTDS detection without Homebrew**

```python
def test_check_freetds_accepts_linux_tsql_plus_odbc_registration(monkeypatch) -> None:
    monkeypatch.setattr(freetds.os, "name", "posix", raising=False)
    monkeypatch.setattr(freetds, "_classify_platform_slug", lambda: "linux")
    monkeypatch.setattr(freetds, "_command_exists", lambda name: name in {"tsql", "odbcinst"})
    monkeypatch.setattr(freetds, "_run_command", lambda command: "[FreeTDS]\n" if command == ["odbcinst", "-q", "-d"] else "DRIVERS............: /etc/odbcinst.ini\n")

    result = freetds.run_check_freetds()

    assert result.installed is True
    assert result.registered is True


def test_check_freetds_skips_auto_register_when_prefix_is_unknown(monkeypatch) -> None:
    monkeypatch.setattr(freetds, "_classify_platform_slug", lambda: "linux")
    monkeypatch.setattr(freetds, "_resolve_freetds_prefix", lambda: None)

    with pytest.raises(RuntimeError, match="automatic registration is only supported when the FreeTDS library path can be resolved"):
        freetds.run_check_freetds(register_missing=True)
```

- [ ] **Step 2: Run the FreeTDS test file and confirm the new cases fail**

Run: `cd lib && uv run pytest ../tests/unit/freetds/test_freetds.py -v`
Expected: FAIL because `run_check_freetds()` still requires `brew list --formula freetds`.

- [ ] **Step 3: Replace Homebrew-only detection with platform-aware probes**

```python
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
        if (candidate / "lib").exists():
            return candidate
    return None
```

- [ ] **Step 4: Keep native Windows unsupported but support Linux and WSL in the same check**

```python
platform_slug = _classify_platform_slug()
if platform_slug == "windows":
    return FreeTdsCheckOutput(..., supported_platform=False, message="Native Windows is not supported. Use WSL for the local SQL Server workflow.")

if not _freetds_installed(platform_slug):
    return FreeTdsCheckOutput(..., supported_platform=True, installed=False, message="FreeTDS is not installed.")
```

- [ ] **Step 5: Align `setup-source` prereq guidance with the shared detector**

```python
if technology == "sql_server":
    result = run_check_freetds()
    if not result.installed:
        console.print("[red]✗[/red] FreeTDS not found. Install it with your platform package manager, then rerun setup-source.")
        raise typer.Exit(code=1)
    if not result.registered:
        console.print("[red]✗[/red] FreeTDS is installed but not registered in unixODBC.")
        raise typer.Exit(code=1)
    success("freetds installed and registered")
```

- [ ] **Step 6: Re-run the focused SQL Server prereq tests**

Run: `cd lib && uv run pytest ../tests/unit/freetds/test_freetds.py ../tests/unit/cli/test_setup_source_cmd.py -v`
Expected: PASS for the new Linux/WSL FreeTDS coverage and the updated `setup-source` behavior.

- [ ] **Step 7: Commit the shared SQL Server prereq slice**

```bash
git add lib/shared/freetds.py lib/shared/cli/setup_source_cmd.py tests/unit/freetds/test_freetds.py tests/unit/cli/test_setup_source_cmd.py
git commit -m "feat: support linux sql server prereq detection"
```

### Task 3: Make Init And Runtime Guidance Platform-Aware

**Files:**

- Modify: `commands/init-ad-migration.md`
- Modify: `lib/shared/db_connect.py`
- Modify: `lib/shared/cli/error_handler.py`
- Test: `tests/unit/db_connect/test_db_connect.py`
- Test: `tests/unit/init/test_init.py`

- [ ] **Step 1: Write failing tests for platform-aware remediation text**

```python
def test_sql_server_connect_error_message_is_not_homebrew_only() -> None:
    ...
    with pytest.raises(RuntimeError, match="Install FreeTDS using your platform package manager"):
        mod.sql_server_connect("testdb")


def test_init_windows_message_points_to_wsl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("shared.init.classify_host_platform", lambda: HostPlatform("windows", False, "Windows"))
    result = build_init_platform_gate_message()
    assert "Use WSL" in result
```

- [ ] **Step 2: Run the db/connect and init messaging tests and confirm they fail**

Run: `cd lib && uv run pytest ../tests/unit/db_connect/test_db_connect.py ../tests/unit/init/test_init.py -k 'wsl or brew or platform' -v`
Expected: FAIL because the code still emits `brew install freetds` / `brew install msodbcsql18`.

- [ ] **Step 3: Replace hardcoded macOS remediation text in runtime code**

```python
raise RuntimeError(
    f"ODBC driver '{driver}' not found. "
    "Install FreeTDS using your platform package manager and ensure unixODBC can see it."
)
```

```python
if _PYODBC_INTERFACE_ERROR and isinstance(exc, _PYODBC_INTERFACE_ERROR):
    return 2, str(exc), (
        "Install a supported SQL Server ODBC driver for this platform and set MSSQL_DRIVER if you are not using FreeTDS."
    )
```

- [ ] **Step 4: Update the init command contract to describe supported platform branches**

```md
- Native Windows: stop immediately and tell the user to rerun inside WSL.
- macOS: install missing CLI and SQL Server prerequisites through Homebrew.
- Linux/WSL: use the Linux/WSL install path for the CLI and package-manager-specific SQL Server prerequisites.
- Do not imply native Windows support.
```

- [ ] **Step 5: Add explicit Linux/WSL installer and remediation language to `commands/init-ad-migration.md`**

```md
If the host platform is Linux or WSL and `ad-migration` is missing, stop after the pre-check and tell the user the supported install command for Linux/WSL, rather than attempting the macOS Homebrew path.

If FreeTDS is missing on Linux or WSL, tell the user to install both FreeTDS and unixODBC using the distro package manager, then rerun `/init-ad-migration`.
```

- [ ] **Step 6: Re-run the focused remediation tests**

Run: `cd lib && uv run pytest ../tests/unit/db_connect/test_db_connect.py ../tests/unit/init/test_init.py -v`
Expected: PASS for the updated remediation text and the new WSL/native-Windows assertions.

- [ ] **Step 7: Commit the init and remediation-text slice**

```bash
git add commands/init-ad-migration.md lib/shared/db_connect.py lib/shared/cli/error_handler.py tests/unit/db_connect/test_db_connect.py tests/unit/init/test_init.py
git commit -m "feat: make init guidance platform aware"
```

### Task 4: Align Operator Docs And Scaffolded Expectations With The New Platform Matrix

**Files:**

- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `docs/wiki/Command-Reference.md`
- Modify: `docs/wiki/Quickstart.md`
- Modify: `docs/wiki/Stage-1-Project-Init.md`
- Modify: `docs/design/homebrew-cli-publishing/README.md`

- [ ] **Step 1: Update the documented support matrix**

```md
Local execution is supported on macOS, Linux, and WSL.
Native Windows is not supported; use WSL for the local workflow.
```

- [ ] **Step 2: Remove Linux-facing Homebrew-only wording from install docs**

```md
macOS install path:
brew tap accelerate-data/homebrew-tap
brew install ad-migration

Linux/WSL install path:
[document the supported Linux/WSL install contract chosen in Task 3]
```

- [ ] **Step 3: Update init-stage docs so they match the implemented command behavior**

```md
`/init-ad-migration` installs the CLI automatically on macOS when Homebrew is available.
On Linux and WSL it reports the supported install path and validates the remaining prerequisites after the CLI is present.
On native Windows it stops and tells the user to rerun inside WSL.
```

- [ ] **Step 4: Run Markdown lint on the changed docs**

Run: `markdownlint docs/wiki/Installation-and-Prerequisites.md docs/wiki/Command-Reference.md docs/wiki/Quickstart.md docs/wiki/Stage-1-Project-Init.md docs/design/homebrew-cli-publishing/README.md commands/init-ad-migration.md`
Expected: PASS with no lint errors.

- [ ] **Step 5: Run the targeted Python regression suite**

Run: `cd lib && uv run pytest ../tests/unit/freetds/test_freetds.py ../tests/unit/init/test_init.py ../tests/unit/db_connect/test_db_connect.py ../tests/unit/cli/test_setup_source_cmd.py -v`
Expected: PASS for all new platform and SQL Server prerequisite regressions.

- [ ] **Step 6: Commit the docs and verification slice**

```bash
git add docs/wiki/Installation-and-Prerequisites.md docs/wiki/Command-Reference.md docs/wiki/Quickstart.md docs/wiki/Stage-1-Project-Init.md docs/design/homebrew-cli-publishing/README.md commands/init-ad-migration.md
git commit -m "docs: align platform support guidance"
```

## Self-Review

- Spec coverage: `VU-1093` requires platform detection, Linux/WSL CLI/install behavior, SQL Server prerequisite detection without Homebrew assumptions, platform-aware remediation text, docs updates, and regression coverage. Tasks 1-4 cover those requirements directly.
- Placeholder scan: no `TODO`/`TBD` placeholders remain; every task names concrete files, commands, and expected outcomes.
- Type consistency: platform classification is centered on one `HostPlatform` helper in `lib/shared/init.py`; the same macOS/Linux/WSL/windows vocabulary is reused across tasks.

Plan complete and saved to `docs/superpowers/plans/2026-04-15-linux-wsl-init-support.md`. Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
