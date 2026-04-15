# Homebrew CLI Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a macOS Homebrew install path for `ad-migration` that installs machine prerequisites, exposes only the public CLI, and keeps `/init-ad-migration` as the supported workflow entrypoint.

**Architecture:** Convert the current `lib` Python project into the reusable `ad-migration-shared` distribution, add thin `ad-migration-cli` and `ad-migration-internal` wrapper projects in this repo, then release the public CLI plus shared artifact to GitHub Releases and teach the custom tap formula to install both. Keep plugin command behavior authoritative in `commands/init-ad-migration.md`, but switch plugin-maintainer paths and repo metadata to the internal project so Homebrew users do not see plugin-only console scripts.

**Tech Stack:** Python 3.11+, uv, Hatchling, Typer, pytest, GitHub Actions, Homebrew custom tap

---

## File Structure

- Modify: `lib/pyproject.toml`
  - Rebrand the existing project as `ad-migration-shared`, keep the `shared` import package, remove public console scripts, and preserve optional dependency groups used by internal tooling.
- Create: `packages/ad-migration-cli/pyproject.toml`
  - Public distribution metadata; depends on `ad-migration-shared` and exposes only `ad-migration`.
- Create: `packages/ad-migration-cli/src/ad_migration_cli/__init__.py`
  - Minimal package marker for the public distribution.
- Create: `packages/ad-migration-cli/src/ad_migration_cli/main.py`
  - Thin wrapper that re-exports the existing public Typer app from `shared.cli.main`.
- Create: `packages/ad-migration-internal/pyproject.toml`
  - Internal distribution metadata; depends on `ad-migration-shared` and exposes plugin-only scripts such as `discover`, `setup-ddl`, `migrate`, `profile`, `test-harness`, `migrate-util`, `generate-sources`, `refactor`, and `catalog-enrich`.
- Create: `packages/ad-migration-internal/src/ad_migration_internal/__init__.py`
  - Minimal package marker for the internal distribution.
- Create: `packages/ad-migration-internal/src/ad_migration_internal/entrypoints.py`
  - Thin wrapper module that imports existing Typer apps from `shared.*`.
- Modify: `commands/init-ad-migration.md`
  - Keep the Homebrew install contract, make the macOS-only scope explicit, and switch repo-local `uv run --project ...` paths to the internal project.
- Modify: `commands/generate-tests.md`
- Modify: `commands/generate-model.md`
- Modify: `commands/refactor.md`
- Modify: `commands/status.md`
  - Update internal `uv run --project` examples to point at `packages/ad-migration-internal`.
- Modify: `skills/**` references that currently point at `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" ...`
  - Keep all plugin-maintainer command examples consistent with the new internal project path.
- Modify: `docs/reference/cli-testing/README.md`
  - Update maintainer test/smoke commands for the split package layout.
- Modify: `repo-map.json`
  - Add the new package directories, release workflow, and updated maintainer commands.
- Create: `.github/workflows/release-cli.yml`
  - Build and upload `ad-migration-cli` and `ad-migration-shared` release artifacts on tags; smoke-test the public executable before upload.
- Create: `.github/actions/update-homebrew-tap/` or equivalent script under `scripts/`
  - Shared logic to clone/update `accelerate-data/homebrew-tap`, rewrite `Formula/ad-migration.rb`, and commit checksum updates.
- Create: `tests/unit/repo_structure/test_python_package_layout.py`
  - Assert the new package directories exist and the public/internal script exposure stays split correctly.
- Modify: `tests/unit/repo_structure/test_root_plugin_layout.py`
  - Extend structural regression coverage for the new package directories.
- Create: `tests/unit/cli/test_packaging_contract.py`
  - Validate TOML metadata: public package exports only `ad-migration`, internal package exports plugin-only scripts, shared package exports none.
- External modify: `accelerate-data/homebrew-tap/Formula/ad-migration.rb`
  - Install the public CLI release artifact plus the shared release artifact and durable Homebrew prerequisites.
- External modify: `accelerate-data/homebrew-tap/README.md`
  - Document tap usage if the scaffolded README is still boilerplate.

### Task 1: Split Python Packaging Into Shared, Public, And Internal Distributions

**Files:**

- Modify: `lib/pyproject.toml`
- Create: `packages/ad-migration-cli/pyproject.toml`
- Create: `packages/ad-migration-cli/src/ad_migration_cli/__init__.py`
- Create: `packages/ad-migration-cli/src/ad_migration_cli/main.py`
- Create: `packages/ad-migration-internal/pyproject.toml`
- Create: `packages/ad-migration-internal/src/ad_migration_internal/__init__.py`
- Create: `packages/ad-migration-internal/src/ad_migration_internal/entrypoints.py`
- Test: `tests/unit/cli/test_packaging_contract.py`

- [ ] **Step 1: Write the failing packaging-contract test**

```python
from __future__ import annotations

import tomllib
from pathlib import Path


def _load_pyproject(relative_path: str) -> dict:
    repo_root = Path(__file__).resolve().parents[3]
    with (repo_root / relative_path).open("rb") as handle:
        return tomllib.load(handle)


def test_package_scripts_are_split_by_contract() -> None:
    shared = _load_pyproject("lib/pyproject.toml")
    public_cli = _load_pyproject("packages/ad-migration-cli/pyproject.toml")
    internal = _load_pyproject("packages/ad-migration-internal/pyproject.toml")

    assert shared["project"]["name"] == "ad-migration-shared"
    assert "scripts" not in shared["project"]

    assert public_cli["project"]["name"] == "ad-migration-cli"
    assert public_cli["project"]["scripts"] == {
        "ad-migration": "ad_migration_cli.main:app",
    }
    assert "ad-migration-shared" in public_cli["project"]["dependencies"]

    assert internal["project"]["name"] == "ad-migration-internal"
    assert "ad-migration" not in internal["project"]["scripts"]
    assert internal["project"]["scripts"]["discover"] == "ad_migration_internal.entrypoints:discover_app"
    assert internal["project"]["scripts"]["setup-ddl"] == "ad_migration_internal.entrypoints:setup_ddl_app"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/cli/test_packaging_contract.py -v`

Expected: FAIL with `FileNotFoundError` for `packages/ad-migration-cli/pyproject.toml` and/or assertion failures because `lib/pyproject.toml` still exposes scripts.

- [ ] **Step 3: Implement the package split with thin wrappers**

```toml
# lib/pyproject.toml
[project]
name = "ad-migration-shared"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "jsonschema>=4.18",
  "pydantic>=2.0",
  "pyyaml>=6.0",
  "rich>=13.0",
  "sqlglot>=25.0,<26",
  "typer>=0.12",
]

[project.optional-dependencies]
dev = ["pytest>=9.0.3", "jsonschema>=4.18", "pyodbc>=5.0", "oracledb>=2.0"]
export = ["pyodbc>=5.0"]
oracle = ["oracledb>=2.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["shared"]
```

```toml
# packages/ad-migration-cli/pyproject.toml
[project]
name = "ad-migration-cli"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "ad-migration-shared==0.1.0",
]

[project.scripts]
ad-migration = "ad_migration_cli.main:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ad_migration_cli"]
```

```python
# packages/ad-migration-cli/src/ad_migration_cli/main.py
from __future__ import annotations

from shared.cli.main import app

__all__ = ["app"]
```

```toml
# packages/ad-migration-internal/pyproject.toml
[project]
name = "ad-migration-internal"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "ad-migration-shared==0.1.0",
]

[project.scripts]
catalog-enrich = "ad_migration_internal.entrypoints:catalog_enrich_app"
discover = "ad_migration_internal.entrypoints:discover_app"
generate-sources = "ad_migration_internal.entrypoints:generate_sources_app"
init = "ad_migration_internal.entrypoints:init_app"
migrate = "ad_migration_internal.entrypoints:migrate_app"
migrate-util = "ad_migration_internal.entrypoints:migrate_util_app"
profile = "ad_migration_internal.entrypoints:profile_app"
refactor = "ad_migration_internal.entrypoints:refactor_app"
setup-ddl = "ad_migration_internal.entrypoints:setup_ddl_app"
test-harness = "ad_migration_internal.entrypoints:test_harness_app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/ad_migration_internal"]
```

```python
# packages/ad-migration-internal/src/ad_migration_internal/entrypoints.py
from __future__ import annotations

from shared.catalog_enrich import app as catalog_enrich_app
from shared.discover import app as discover_app
from shared.generate_sources import app as generate_sources_app
from shared.init import app as init_app
from shared.migrate import app as migrate_app
from shared.dry_run import app as migrate_util_app
from shared.profile import app as profile_app
from shared.refactor import app as refactor_app
from shared.setup_ddl import app as setup_ddl_app
from shared.test_harness import app as test_harness_app
```

- [ ] **Step 4: Run the targeted test and package builds**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli/test_packaging_contract.py -v
cd ../packages/ad-migration-cli && python3 -m build
cd ../ad-migration-internal && python3 -m build
```

Expected: the pytest file passes and both wrapper projects build a wheel and sdist successfully.

- [ ] **Step 5: Commit**

```bash
git add lib/pyproject.toml \
  packages/ad-migration-cli \
  packages/ad-migration-internal \
  tests/unit/cli/test_packaging_contract.py
git commit -m "feat: split public and internal python packages"
```

### Task 2: Move Plugin-Maintainer Commands And Docs To The Internal Project

**Files:**

- Modify: `commands/init-ad-migration.md`
- Modify: `commands/generate-model.md`
- Modify: `commands/generate-tests.md`
- Modify: `commands/refactor.md`
- Modify: `commands/status.md`
- Modify: `skills/README.md`
- Modify: `skills/listing-objects/SKILL.md`
- Modify: `skills/analyzing-table/**`
- Modify: `skills/profiling-table/**`
- Modify: `skills/generating-model/SKILL.md`
- Modify: `skills/generating-tests/**`
- Modify: `skills/reviewing-model/SKILL.md`
- Modify: `skills/reviewing-tests/**`
- Modify: `skills/refactoring-sql/**`
- Modify: `docs/reference/cli-testing/README.md`
- Modify: `repo-map.json`
- Test: `tests/unit/repo_structure/test_root_plugin_layout.py`
- Test: `tests/unit/repo_structure/test_python_package_layout.py`

- [ ] **Step 1: Write failing structure/documentation tests**

```python
from __future__ import annotations

import json
from pathlib import Path


def test_repo_exposes_split_python_projects() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    assert (repo_root / "packages" / "ad-migration-cli" / "pyproject.toml").is_file()
    assert (repo_root / "packages" / "ad-migration-internal" / "pyproject.toml").is_file()


def test_repo_map_mentions_internal_project_commands() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    with (repo_root / "repo-map.json").open("rb") as handle:
        repo_map = json.load(handle)

    assert repo_map["commands"]["init_check_freetds"].startswith("cd packages/ad-migration-internal")
    assert repo_map["commands"]["migrate_util_status"].startswith("cd packages/ad-migration-internal")
```

- [ ] **Step 2: Run the structure tests to verify they fail**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_root_plugin_layout.py ../tests/unit/repo_structure/test_python_package_layout.py -v`

Expected: FAIL because the new package directories and repo-map command updates do not exist yet.

- [ ] **Step 3: Update command docs, skills, reference docs, and repo-map**

```md
# commands/init-ad-migration.md
If the host platform is Linux, stop after the pre-check and tell the user the Homebrew auto-install path is currently supported only on macOS. Do not attempt `brew install ad-migration` on Linux in v1.

uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" init check-freetds
uv sync --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" --extra export
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" setup-ddl write-partial-manifest --project-root . ...
```

```json
// repo-map.json command examples
"init_check_freetds": "cd packages/ad-migration-internal && uv run init check-freetds [--register-missing]",
"migrate_util_status": "cd packages/ad-migration-internal && uv run migrate-util status [<fqn>]",
"generate_sources": "cd packages/ad-migration-internal && uv run generate-sources [--write] [--strict] [--project-root <path>]"
```

Update every skill/reference file returned by the `rg` search so maintainers never reach for the Homebrew-public package to run plugin-only commands.

- [ ] **Step 4: Run the updated structure/documentation tests and targeted markdown lint**

Run:

```bash
cd lib && uv run pytest ../tests/unit/repo_structure/test_root_plugin_layout.py ../tests/unit/repo_structure/test_python_package_layout.py -v
markdownlint commands/init-ad-migration.md commands/generate-model.md commands/generate-tests.md commands/refactor.md commands/status.md docs/reference/cli-testing/README.md
```

Expected: pytest passes, markdownlint passes, and `rg '"${CLAUDE_PLUGIN_ROOT}/lib"' commands skills docs/reference repo-map.json` returns only deliberate shared-library test paths that are still valid.

- [ ] **Step 5: Commit**

```bash
git add commands/generate-model.md \
  commands/generate-tests.md \
  commands/init-ad-migration.md \
  commands/refactor.md \
  commands/status.md \
  docs/reference/cli-testing/README.md \
  repo-map.json \
  skills \
  tests/unit/repo_structure/test_root_plugin_layout.py \
  tests/unit/repo_structure/test_python_package_layout.py
git commit -m "refactor: point plugin tooling at internal package"
```

### Task 3: Add Release Builds For Public CLI And Shared Artifacts

**Files:**

- Create: `.github/workflows/release-cli.yml`
- Modify: `repo-map.json`
- Test: local build commands from `lib/` and `packages/ad-migration-cli/`

- [ ] **Step 1: Write a failing workflow smoke check**

Add a small assertion to `tests/unit/repo_structure/test_python_package_layout.py` that checks the new workflow exists:

```python
def test_release_workflow_exists() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    assert (repo_root / ".github" / "workflows" / "release-cli.yml").is_file()
```

- [ ] **Step 2: Run the repo-structure test to verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py::test_release_workflow_exists -v`

Expected: FAIL because `.github/workflows/release-cli.yml` is missing.

- [ ] **Step 3: Implement the release workflow**

```yaml
name: Release CLI

on:
  push:
    tags:
      - "v*"

permissions:
  contents: write

jobs:
  build-artifacts:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - name: Build shared artifacts
        run: |
          python -m pip install build
          cd lib
          python -m build
      - name: Build public CLI artifacts
        run: |
          cd packages/ad-migration-cli
          python -m build
      - name: Smoke-test public CLI in fresh venv
        run: |
          python -m venv /tmp/ad-migration-release
          source /tmp/ad-migration-release/bin/activate
          python -m pip install lib/dist/ad_migration_shared-*.whl
          python -m pip install packages/ad-migration-cli/dist/ad_migration_cli-*.whl
          ad-migration --version
      - name: Upload release artifacts
        uses: softprops/action-gh-release@v2
        with:
          files: |
            lib/dist/*
            packages/ad-migration-cli/dist/*
```

Update `repo-map.json` to list the new workflow under `build_systems.config_files`.

- [ ] **Step 4: Run local build/smoke commands**

Run:

```bash
python3 -m pip install build
cd lib && python3 -m build
cd ../packages/ad-migration-cli && python3 -m build
python3 -m venv /tmp/ad-migration-release
source /tmp/ad-migration-release/bin/activate
python -m pip install /Users/hbanerjee/src/migration-utility/lib/dist/ad_migration_shared-*.whl
python -m pip install /Users/hbanerjee/src/migration-utility/packages/ad-migration-cli/dist/ad_migration_cli-*.whl
ad-migration --version
```

Expected: both builds succeed and the fresh venv prints the CLI version.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release-cli.yml repo-map.json tests/unit/repo_structure/test_python_package_layout.py
git commit -m "ci: publish shared and public cli artifacts"
```

### Task 4: Automate Tap Formula Updates And Publish The Homebrew Contract

**Files:**

- Create: `scripts/update_homebrew_tap.py`
- Create: `scripts/templates/homebrew/Formula/ad-migration.rb.tmpl`
- Modify: `.github/workflows/release-cli.yml`
- External modify: `accelerate-data/homebrew-tap/Formula/ad-migration.rb`
- External modify: `accelerate-data/homebrew-tap/README.md`

- [ ] **Step 1: Write the failing tap-update test**

Create a pure unit test that renders the formula template from fixture inputs:

```python
from __future__ import annotations

from pathlib import Path

from scripts.update_homebrew_tap import render_formula


def test_render_formula_includes_shared_resource() -> None:
    formula = render_formula(
        version="0.1.0",
        cli_url="https://example.test/ad_migration_cli-0.1.0.tar.gz",
        cli_sha256="cli-sha",
        shared_url="https://example.test/ad_migration_shared-0.1.0.tar.gz",
        shared_sha256="shared-sha",
    )

    assert 'depends_on "freetds"' in formula
    assert 'depends_on "unixodbc"' in formula
    assert 'resource "ad-migration-shared"' in formula
    assert "ad_migration_shared-0.1.0.tar.gz" in formula
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_homebrew_formula_template.py -v`

Expected: FAIL with `ModuleNotFoundError` because `scripts.update_homebrew_tap` and the template do not exist.

- [ ] **Step 3: Implement the formula template and tap updater**

```ruby
# scripts/templates/homebrew/Formula/ad-migration.rb.tmpl
class AdMigration < Formula
  include Language::Python::Virtualenv

  desc "Warehouse stored procedure to dbt migration CLI"
  homepage "https://github.com/accelerate-data/migration-utility"
  url "<%= cli_url %>"
  sha256 "<%= cli_sha256 %>"
  license "MIT"

  depends_on "python@3.12"
  depends_on "freetds"
  depends_on "unixodbc"

  resource "ad-migration-shared" do
    url "<%= shared_url %>"
    sha256 "<%= shared_sha256 %>"
  end

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/ad-migration --version")
  end
end
```

```python
# scripts/update_homebrew_tap.py
def render_formula(version: str, cli_url: str, cli_sha256: str, shared_url: str, shared_sha256: str) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    return (
        template.replace("<%= cli_url %>", cli_url)
        .replace("<%= cli_sha256 %>", cli_sha256)
        .replace("<%= shared_url %>", shared_url)
        .replace("<%= shared_sha256 %>", shared_sha256)
        .replace('version.to_s', f'"{version}"')
    )
```

Then extend `.github/workflows/release-cli.yml` to:

- compute SHA256 for the public CLI sdist and shared sdist
- clone `accelerate-data/homebrew-tap`
- rewrite `Formula/ad-migration.rb`
- commit and push the formula update with a bot identity

- [ ] **Step 4: Run the template test and dry-run the script locally**

Run:

```bash
cd lib && uv run pytest ../tests/unit/repo_structure/test_homebrew_formula_template.py -v
python3 scripts/update_homebrew_tap.py \
  --version 0.1.0 \
  --cli-url https://example.test/ad_migration_cli-0.1.0.tar.gz \
  --cli-sha256 1111111111111111111111111111111111111111111111111111111111111111 \
  --shared-url https://example.test/ad_migration_shared-0.1.0.tar.gz \
  --shared-sha256 2222222222222222222222222222222222222222222222222222222222222222 \
  --output /tmp/ad-migration.rb
```

Expected: pytest passes and `/tmp/ad-migration.rb` contains both the public artifact URL and the shared resource URL.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/release-cli.yml \
  scripts/update_homebrew_tap.py \
  scripts/templates/homebrew/Formula/ad-migration.rb.tmpl \
  tests/unit/repo_structure/test_homebrew_formula_template.py
git commit -m "ci: automate homebrew tap formula updates"
```

### Task 5: Verify `/init-ad-migration` And The Published Formula Contract End-To-End

**Files:**

- Modify: `commands/init-ad-migration.md`
- Modify: `docs/reference/cli-testing/README.md`
- External modify: `accelerate-data/homebrew-tap/Formula/ad-migration.rb`
- Test: local Homebrew formula install in the tap repo

- [ ] **Step 1: Add the final contract checks to the maintainer docs**

```md
# docs/reference/cli-testing/README.md
1. Build `ad-migration-shared` and `ad-migration-cli`.
2. Confirm `ad-migration --version` works in a fresh venv.
3. In a clone of `accelerate-data/homebrew-tap`, run:
   `brew install --build-from-source Formula/ad-migration.rb`
4. Confirm `ad-migration --version` succeeds after the brew install.
5. Run `/init-ad-migration` in a clean plugin checkout and verify the command skips Homebrew install when the brewed binary is already present.
```

- [ ] **Step 2: Run the repository verification commands**

Run:

```bash
cd lib && uv run pytest ../tests/unit/cli ../tests/unit/init ../tests/unit/repo_structure -v
markdownlint docs/reference/cli-testing/README.md commands/init-ad-migration.md
```

Expected: all targeted unit suites pass and the touched markdown stays clean.

- [ ] **Step 3: Run the external tap verification**

Run in a normal clone of `accelerate-data/homebrew-tap`:

```bash
brew uninstall --force ad-migration || true
brew install --build-from-source Formula/ad-migration.rb
ad-migration --version
brew test ad-migration
```

Expected: Homebrew installs the formula, the `ad-migration` binary prints the released version, and `brew test` passes.

- [ ] **Step 4: Run the workflow entrypoint verification**

Run:

```bash
claude --plugin-dir . --print '/init-ad-migration sql_server'
```

Expected: the printed command contract shows the macOS-only Homebrew install path, uses `packages/ad-migration-internal` for repo-local `uv run` commands, and continues into the existing prerequisite/bootstrap flow after the CLI check.

- [ ] **Step 5: Commit**

```bash
git add commands/init-ad-migration.md docs/reference/cli-testing/README.md
git commit -m "docs: add homebrew verification workflow"
```

## Self-Review

- Spec coverage: package split, Homebrew scope, `/init-ad-migration` install flow, release artifacts, tap updates, and macOS-only messaging each map to at least one task.
- Placeholder scan: the plan avoids `TODO`/`TBD` language and includes concrete file paths, commands, and code snippets for each task.
- Type consistency: the wrapper entrypoint names, project names, and formula resource contract use a consistent `ad-migration-shared` / `ad-migration-cli` / `ad-migration-internal` naming scheme throughout.
