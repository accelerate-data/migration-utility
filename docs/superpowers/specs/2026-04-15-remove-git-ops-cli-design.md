# Remove Git Ops from CLI — Design

**Goal:** Strip all git operations out of the ad-migration CLI and consolidate CLI documentation into a single reference page.

**Architecture:** The CLI is a domain tool — it manages migration state (DDL extraction, catalog, sandbox, reset). Git is the user's concern. Removing git ops eliminates flags that exist only to undo a default the CLI should never have had, and reduces the surface area of every write command.

**Tech Stack:** Python, Typer, pytest

---

## Decision

Git operations (`--no-commit`, `--push`, `is_git_repo`, `stage_and_commit`, `git_push`) are removed from all CLI commands and their tests. `git_ops.py` is deleted. A new single-page CLI reference replaces scattered per-command wiki docs and establishes the recommended git workflow explicitly.

---

## Code changes

### Delete

- `lib/shared/cli/git_ops.py`
- `tests/unit/cli/test_git_ops.py`

### Modify — 7 command files

Remove from each file:

- `from shared.cli.git_ops import ...` import line
- `--no-commit` option (`no_commit: bool = typer.Option(...)`)
- `--push` option (`push: bool = typer.Option(...)`)
- All git logic blocks: `is_git_repo(root)` checks, `stage_and_commit(...)` calls, `git_push(root)` calls, and any associated `warn`/`success`/`error` messages

Files:

| File | Git logic to remove |
|---|---|
| `lib/shared/cli/setup_source_cmd.py` | `--no-commit`, `--push`; git block at end of function |
| `lib/shared/cli/setup_target_cmd.py` | `--no-commit`, `--push`; git block at end of function |
| `lib/shared/cli/setup_sandbox_cmd.py` | `--no-commit`, `--push`; git block after `_write_sandbox_to_manifest` |
| `lib/shared/cli/teardown_sandbox_cmd.py` | `--no-commit`, `--push`; git block inside `if result.status == "ok"` |
| `lib/shared/cli/reset_cmd.py` | `--no-commit`, `--push`; git block in "all" path; git block in per-stage path |
| `lib/shared/cli/exclude_table_cmd.py` | `--no-commit`, `--push`; git block at end of function |
| `lib/shared/cli/add_source_table_cmd.py` | `--no-commit`, `--push`; git block at end of function |

### Modify — 4 test files

Remove all tests that patch `is_git_repo`, `stage_and_commit`, or `git_push`, or that pass `--no-commit` / `--push` flags. Keep all tests for core domain behavior.

| Test file | Tests to remove |
|---|---|
| `tests/unit/cli/test_sandbox_cmds.py` | `test_setup_sandbox_commits_manifest`, `test_setup_sandbox_push_flag_calls_git_push`, `test_setup_sandbox_no_commit_skips_commit`, `test_teardown_sandbox_commits_manifest`, `test_teardown_sandbox_no_commit_skips_commit`, `test_teardown_sandbox_push_flag_calls_git_push` |
| `tests/unit/cli/test_pipeline_cmds.py` | All `*_commit*`, `*_no_commit*`, `*_push*` tests: `test_reset_stage_commits_catalog_files`, `test_reset_stage_no_commit_skips_commit`, `test_reset_all_commits_deleted_paths`, `test_reset_all_no_commit_skips_commit`, `test_reset_stage_push_flag_calls_git_push`, `test_reset_all_push_flag_calls_git_push`, `test_exclude_table_marks_and_commits`, `test_exclude_table_no_commit_flag`, `test_exclude_table_push_flag_calls_git_push`, `test_add_source_table_no_commit_flag`, `test_add_source_table_push_flag_calls_git_push` |
| `tests/unit/cli/test_setup_source_cmd.py` | `test_setup_source_no_commit_flag`, `test_setup_source_push_flag_calls_git_push` |
| `tests/unit/cli/test_setup_target_cmd.py` | `test_setup_target_push_flag_calls_git_push`, `test_setup_target_no_commit` |

---

## Docs changes

### Create: `docs/wiki/CLI-Reference.md`

Single comprehensive CLI reference page. Structure:

1. **Git workflow** (first section) — "Git is your responsibility. Recommended flow: create a feature branch, run your CLI steps, commit the resulting files, open a PR and merge to main." Include the exact files each command writes so the user knows what to commit.
2. **Installation** — `brew install` and `uv run` dev invocation
3. **Setup commands** — `setup-source`, `setup-target`, `setup-sandbox`, `teardown-sandbox` with options, env vars, and what each writes
4. **Pipeline state commands** — `reset`, `exclude-table`, `add-source-table` with options and behavior
5. **Exit codes** — 0 success, 1 domain error, 2 connection/IO error

### Delete

- `docs/wiki/Command-Setup-Source.md` — content absorbed into `CLI-Reference.md`

### Modify

- `docs/wiki/Stage-2-DDL-Extraction.md` — remove `--no-commit` row from options table; remove step 5 "Commits extracted files"
- `docs/wiki/Stage-3-dbt-Scaffolding.md` — remove `--no-commit` row from options table
- `docs/wiki/Testing-the-CLI.md` — remove any mention of `--no-commit` or auto-commit
- `docs/wiki/Command-Reference.md` — update CLI section to link to `CLI-Reference.md` instead of listing options inline; remove "Git workflow scripts" section note about successful items being committed automatically

---

## What does NOT change

- `lib/shared/cli/error_handler.py` — untouched
- All non-git tests — untouched
- `docs/wiki/Git-Workflow.md` — untouched (covers agent/skill multi-table workflow, not CLI)
- `lib/shared/cli/main.py` — untouched
- All domain logic in the 7 command files — untouched
