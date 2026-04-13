# VU-1051 Root Plugin Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the repository root the only Claude plugin root, move tracked plugin source out of `plugin/`, normalize plugin-owned paths around `${CLAUDE_PLUGIN_ROOT}`, and clean up stale local/generated residue plus structural documentation.

**Architecture:** Promote the tracked source tree from `plugin/` to root-level Claude plugin locations in one clean break, then update every repo-owned consumer of those paths. Use a small structural regression test plus grep-based verification so the repo cannot silently drift back to nested `plugin/` source paths.

**Tech Stack:** Claude Code plugin layout, Markdown command/skill specs, Python/uv, pytest, GitHub Actions, promptfoo eval harness

---

## Task 1: Add structural regression coverage for the root plugin layout

**Files:**

- Create: `tests/unit/repo_structure/test_root_plugin_layout.py`

- [x] **Step 1: Write the failing structural test**

```python
from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_root_plugin_layout_is_canonical() -> None:
    assert (REPO_ROOT / ".claude-plugin" / "plugin.json").is_file()
    assert (REPO_ROOT / "commands").is_dir()
    assert (REPO_ROOT / "skills").is_dir()
    assert (REPO_ROOT / "lib" / "pyproject.toml").is_file()
    assert (REPO_ROOT / "mcp" / "ddl" / "server.py").is_file()
    assert not (REPO_ROOT / "plugin").exists()
```

- [x] **Step 2: Run the new test and verify it fails**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_root_plugin_layout.py -v`

Expected: FAIL because `.claude-plugin/plugin.json`, `commands/`, `skills/`, `lib/pyproject.toml`, and `mcp/ddl/server.py` do not exist at the repo root yet and `plugin/` still exists.

- [x] **Step 3: Confirm pytest discovers the new path without repo config changes**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_root_plugin_layout.py -v`

Expected: pytest collects the new test module cleanly and fails only on the intended layout assertions.

- [x] **Step 4: Re-run the test target to confirm the failure is still the layout gap**

Run: `cd lib && uv run pytest ../tests/unit/repo_structure/test_root_plugin_layout.py -v`

Expected: FAIL only on the intended layout assertions, with no import/discovery errors.

- [x] **Step 5: Carry the failing regression forward into Task 2**

Do not checkpoint-commit Task 1 by itself. This test is expected to fail until the root-layout promotion lands. Commit it together with Task 2 once the layout migration makes the tree green.

## Task 2: Promote tracked plugin source to the repository root and remove stale residue

**Files:**

- Create: `.claude-plugin/plugin.json`
- Create: `commands/`
- Create: `skills/`
- Create: `lib/pyproject.toml`
- Create: `lib/shared/`
- Create: `mcp/ddl/server.py`
- Create: `mcp/mssql/tools.yaml`
- Modify: `.mcp.json`
- Modify: `.gitignore`
- Modify: `.envrc`
- Modify: `scripts/worktree.sh`
- Modify: `tests/unit/worktree_script/test_worktree_script.py`
- Delete: the legacy nested plugin manifest, commands, skills, lib, and mcp source tree

- [x] **Step 1: Reconcile the root MCP config before moving files**

```json
{
  "mcpServers": {
    "ddl": {
      "command": "uv",
      "args": ["run", "${CLAUDE_PLUGIN_ROOT}/mcp/ddl/server.py"]
    },
    "mssql": {
      "command": "toolbox",
      "args": ["--tools-file", "${CLAUDE_PLUGIN_ROOT}/mcp/mssql/tools.yaml", "--stdio"]
    },
    "oracle": {
      "command": "sql",
      "args": ["-mcp"]
    }
  }
}
```

- [x] **Step 2: Move tracked plugin assets from the legacy nested tree to root**

Run:

```bash
mv <legacy-plugin-root>/.claude-plugin .claude-plugin
mv <legacy-plugin-root>/commands commands
mv <legacy-plugin-root>/skills skills
mv <legacy-plugin-root>/lib/* lib/
mv <legacy-plugin-root>/mcp/* mcp/
```

Expected: tracked source files now live at root-level plugin locations and `plugin/` only contains stale directories or disappears entirely.

- [x] **Step 3: Update `.gitignore` so it matches the new repo shape**

```gitignore
# Generated migration-project state should not live in the plugin source repo
catalog/

# Python runtime artifacts
__pycache__/
*.py[cod]
.pytest_cache/
.venv/

# Local runtime residue under canonical source roots
lib/.venv/
lib/.pytest_cache/
mcp/ddl/.venv/
mcp/ddl/.pytest_cache/
```

- [x] **Step 4: Make the local repo contract explicit in `.envrc`**

```bash
export CLAUDE_PLUGIN_ROOT=.
```

or equivalent repo-tracked `.envrc` assignment:

```bash
CLAUDE_PLUGIN_ROOT=.
```

- [x] **Step 5: Update the worktree bootstrap script to use the new root lib path**

```bash
local lib_dir="$worktree_path/lib"
if [[ -f "$lib_dir/pyproject.toml" ]]; then
  echo "uv: syncing dev dependencies in $lib_dir"
  (
    cd "$lib_dir" &&
      uv sync --extra dev
  ) || json_error \
    "WORKTREE_UV_SYNC_FAILED" \
    "uv_sync" \
    "uv sync failed while creating the worktree environment." \
    "true" \
    "$0 $branch" \
    "Run 'cd $lib_dir && rm -rf .venv && uv sync --extra dev' to repair the environment, then rerun the worktree command."
fi
```

- [x] **Step 6: Update the worktree script test fixture to create `lib/`, not the legacy nested lib path**

```python
  mkdir -p "$path/lib" "$path/tests/evals"
  printf "[project]\\nname='x'\\n" > "$path/lib/pyproject.toml"
```

- [x] **Step 7: Remove stale local residue after the promoted source tree is in place**

Run:

```bash
rm -rf plugin
rm -rf catalog
find lib -name '__pycache__' -prune -exec rm -rf {} +
find mcp -name '__pycache__' -prune -exec rm -rf {} +
```

Expected: no nested `plugin/` tree remains and no empty/generated root residue survives from the half-migrated state.

- [x] **Step 8: Run focused tests for the moved bootstrap behavior and layout regression**

Run:

```bash
cd lib && uv run pytest ../tests/unit/worktree_script/test_worktree_script.py ../tests/unit/repo_structure/test_root_plugin_layout.py -v
```

Expected: PASS

- [x] **Step 9: Commit the green root-layout slice**

```bash
git add .claude-plugin .mcp.json .gitignore .envrc commands skills lib mcp scripts/worktree.sh tests/unit/worktree_script/test_worktree_script.py tests/unit/repo_structure/test_root_plugin_layout.py
git commit -m "refactor: promote plugin source to the repository root"
```

## Task 3: Rewrite repo-owned references to the new root layout and `${CLAUDE_PLUGIN_ROOT}` contract

**Files:**

- Modify: `AGENTS.md`
- Modify: `README.md`
- Modify: `repo-map.json`
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `docs/wiki/Git-Workflow.md`
- Modify: `docs/tracker.md`
- Modify: `tests/evals/fixtures/README.md`
- Modify: `tests/evals/prompts/cmd-generate-model.txt`
- Modify: `tests/evals/prompts/cmd-generate-tests.txt`
- Modify: `tests/evals/prompts/cmd-live-pipeline.txt`
- Modify: `tests/evals/prompts/cmd-profile.txt`
- Modify: `tests/evals/prompts/cmd-refactor.txt`
- Modify: `tests/evals/prompts/cmd-reset-migration.txt`
- Modify: `tests/evals/prompts/cmd-scope.txt`
- Modify: `tests/evals/prompts/cmd-status.txt`
- Modify: `tests/evals/prompts/skill-analyzing-table.txt`
- Modify: `tests/evals/prompts/skill-analyzing-table-view.txt`
- Modify: `tests/evals/prompts/skill-generating-model.txt`
- Modify: `tests/evals/prompts/skill-generating-tests.txt`
- Modify: `tests/evals/prompts/skill-generating-tests-view.txt`
- Modify: `tests/evals/prompts/skill-listing-objects.txt`
- Modify: `tests/evals/prompts/skill-profiling-table.txt`
- Modify: `tests/evals/prompts/skill-profiling-table-view.txt`
- Modify: `tests/evals/prompts/skill-refactoring-sql.txt`
- Modify: `tests/evals/prompts/skill-refactoring-sql-view.txt`
- Modify: `tests/evals/prompts/skill-reviewing-model.txt`
- Modify: `tests/evals/prompts/skill-reviewing-tests.txt`
- Modify: `tests/evals/prompts/skill-reviewing-tests-view.txt`

- [x] **Step 1: Update structural documentation from the legacy nested paths to the root layout**

```text
  cd lib && uv run pytest
  cd mcp/ddl && uv run pytest
  claude --plugin-dir .
```

- [x] **Step 2: Rewrite repo-map structural entries to the new canonical paths**

```json
{
  "entrypoints": {
    "ddl_mcp_server": "mcp/ddl/server.py",
    "discover_cli": "lib/shared/discover.py",
    "migrate_cli": "lib/shared/migrate.py"
  },
  "key_directories": {
    "skills/": "All plugin skill content at the repository root",
    "commands/": "All plugin command specs at the repository root",
    "lib/": "Python DDL analysis library (uv project, sqlglot-based)",
    "mcp/ddl/": "DDL MCP pytest suite and server"
  }
}
```

- [x] **Step 3: Update CI structural audit directories to match the promoted layout**

```bash
STRUCTURAL_DIRS=(
  "skills/"
  "commands/"
  "lib/shared/"
  "mcp/ddl/"
  "tests/unit/"
)
```

- [x] **Step 4: Remove eval harness overrides that rewrite `${CLAUDE_PLUGIN_ROOT}` away from the repo root**

```text
Keep `${CLAUDE_PLUGIN_ROOT}` references as written.
Pass `--project-root {{run_path}}` to CLI commands that accept it.
```

- [x] **Step 5: Fix moved Markdown cross-links inside commands and skills**

```text
Before in skills/generating-model/SKILL.md:
  [../../lib/shared/generate_model_error_codes.md](../../lib/shared/generate_model_error_codes.md)

After:
  [../lib/shared/generate_model_error_codes.md](../lib/shared/generate_model_error_codes.md)
```

- [x] **Step 6: Run a repo-wide grep audit for stale nested source-path references**

Run:

```bash
rg -n "plugin/(\\.claude-plugin|commands|skills|lib|mcp)" .github AGENTS.md README.md repo-map.json docs tests scripts commands skills lib mcp
```

Expected: no matches that refer to current source-tree layout; any remaining hits must be intentionally historical text and should be removed if not required.

- [x] **Step 7: Commit the path rewrite sweep**

```bash
git add AGENTS.md README.md repo-map.json .github/workflows/ci.yml docs tests commands skills
git commit -m "docs: rewrite plugin paths for root layout"
```

## Task 4: Verify end-to-end behavior from the new layout

**Files:**

- Modify: `README.md`
- Modify: `AGENTS.md`
- Modify: `repo-map.json`

- [x] **Step 1: Recreate clean virtual environments in the canonical root locations**

Run:

```bash
rm -rf lib/.venv mcp/ddl/.venv
cd lib && uv sync --extra dev
cd ../mcp/ddl && uv sync
```

Expected: `lib/.venv` and `mcp/ddl/.venv` are recreated with valid interpreter paths under the root layout.

- [x] **Step 2: Run the shared-library test suite from the new root layout**

Run: `cd lib && uv run pytest`

Expected: PASS

- [x] **Step 3: Run the DDL MCP test suite from the new root layout**

Run: `cd mcp/ddl && uv run pytest`

Expected: PASS

- [x] **Step 4: Run a root plugin smoke check**

Run:

```bash
claude --plugin-dir . --print "Reply with the loaded plugin names only."
```

Expected: the local plugin loads from the repository root without requiring `plugin/`.

- [x] **Step 5: Re-run the stale-path audit and confirm the repo is clean**

Run:

```bash
git status --short
rg -n "plugin/(\\.claude-plugin|commands|skills|lib|mcp)" .github AGENTS.md README.md repo-map.json docs tests scripts commands skills lib mcp
```

Expected: only intentional modified files appear in `git status`, and the grep command returns no current-layout matches.

- [x] **Step 6: Commit the verified cleanup**

```bash
git add .gitignore README.md AGENTS.md repo-map.json
git commit -m "chore: verify root plugin layout cleanup"
```

## Follow-Up Slice: Path-Contract Repair

- [x] Replace the repo-root `.envrc` `CLAUDE_PLUGIN_ROOT=.` contract with an absolute direnv-loaded value.
- [x] Update `lib/shared/init_templates.py` so scaffolded `scripts/worktree.sh` bootstraps `lib/` at the worktree root, and extend init coverage to lock that path.
- [x] Reconcile `repo-map.json` from the legacy nested source paths to the root layout.
- [x] Remove eval prompt overrides that bypassed the repo-root `${CLAUDE_PLUGIN_ROOT}` contract, and point repo-owned prompt references at root `commands/` and `skills/`.
- [x] Refresh `tests/evals/fixtures/README.md` so repo-owned extraction examples use the root layout.
- [x] Verification for this slice: run the focused pytest targets plus the targeted grep audit over `repo-map.json`, `tests/evals`, `lib/shared/init_templates.py`, `tests/unit/init`, and `tests/evals/fixtures/README.md`.

## Current State

- [x] Tasks 1 and 2 are functionally complete and green.
- [x] Task 3 reference rewrites are complete and the stale nested-path audit is clean.
- [x] First checkpoint commit created for the root-layout migration slice.
- [x] Task 4 final verification and cleanup commit created.

## Self-Review

- Spec coverage: the plan covers root manifest promotion, tracked source moves, `${CLAUDE_PLUGIN_ROOT}` normalization, `.gitignore` cleanup, local residue removal, CI/docs/eval updates, and end-to-end verification.
- Placeholder scan: no `TODO`, `TBD`, or “update as needed” placeholders remain; each task names concrete files and commands.
- Type consistency: all path references use the same target layout (`.claude-plugin/`, `commands/`, `skills/`, `lib/`, `mcp/ddl/`) and the same environment contract (`${CLAUDE_PLUGIN_ROOT}` for plugin-owned paths, repo-relative paths for source-tree metadata).
