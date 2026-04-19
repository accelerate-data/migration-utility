# Migrate Mart Coordinator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a customer-facing whole-scope `/migrate-mart-plan` and `/migrate-mart` workflow that plans, resumes, and coordinates mart migration through deterministic worktrees and stage PRs.

**Architecture:** Introduce shared plugin scripts under `scripts/` for deterministic worktree, PR, merge, and cleanup behavior. Update existing stage slash commands to use those helpers, remove human PR prompts, and support positional coordinator mode. Add planner/coordinator slash commands that use a Markdown operational plan under `docs/migration-plans/<slug>/README.md` as the source of truth.

**Tech Stack:** Claude Code plugin command Markdown, shell helper scripts, existing `ad-migration` Python CLI, GitHub CLI, pytest shell-script tests, Promptfoo command evals.

---

## Execution Model

Use parallel implementation subagents only across disjoint workstreams and separate worktrees. Within each workstream, follow `subagent-driven-development`: implementation subagent, spec-compliance review, then code-quality review before integrating that workstream.

The `subagent-driven-development` skill forbids multiple implementation subagents editing the same state concurrently. These workstreams are intentionally separated by write scope so they can run in parallel, then be integrated sequentially by the coordinator.

Create implementation worktrees from the repo root using the maintainer helper:

```bash
./scripts/worktree.sh feature/migrate-mart-shared-scripts
./scripts/worktree.sh feature/migrate-mart-stage-commands
./scripts/worktree.sh feature/migrate-mart-planner
./scripts/worktree.sh feature/migrate-mart-coordinator
./scripts/worktree.sh feature/migrate-mart-evals-docs
```

Parallel wave:

- Workstream A: Shared plugin scripts and shell tests.
- Workstream B: Existing stage command contract updates.
- Workstream C: `/migrate-mart-plan` command.
- Workstream D: `/migrate-mart` command.
- Workstream E: Evals, docs, and repo-map updates.

Integration order:

1. A, because B-D call the shared helpers.
2. B, because C-D rely on stage command coordinator mode.
3. C and D, after A/B are integrated.
4. E last, after command text stabilizes.

After each integrated workstream, run its focused tests. After all workstreams, run:

```bash
cd lib && uv run pytest ../tests/unit/worktree_script ../tests/unit/repo_structure
cd tests/evals && npm run eval:cmd-scope
cd tests/evals && npm run eval:cmd-profile
cd tests/evals && npm run eval:cmd-generate-tests
cd tests/evals && npm run eval:cmd-refactor
cd tests/evals && npm run eval:cmd-generate-model
cd tests/evals && npm run eval:cmd-refactor-mart
cd tests/evals && npm run eval:smoke
markdownlint docs/design/migrate-mart-coordinator/README.md docs/superpowers/plans/2026-04-19-migrate-mart-coordinator.md
```

## File Structure

### Workstream A: Shared Plugin Scripts

- Create: `scripts/stage-worktree.sh`
- Create: `scripts/stage-pr.sh`
- Create: `scripts/stage-pr-merge.sh`
- Create: `scripts/stage-cleanup.sh`
- Create: `scripts/README.md`
- Modify: `tests/unit/worktree_script/test_worktree_script.py`
- Create: `tests/unit/worktree_script/test_stage_pr_script.py`
- Create: `tests/unit/worktree_script/test_stage_pr_merge_script.py`
- Create: `tests/unit/worktree_script/test_stage_cleanup_script.py`
- Modify: `tests/unit/repo_structure/test_root_plugin_layout.py`

### Workstream B: Stage Command Contract

- Modify: `commands/scope-tables.md`
- Modify: `commands/profile-tables.md`
- Modify: `commands/generate-tests.md`
- Modify: `commands/refactor-query.md`
- Modify: `commands/generate-model.md`
- Modify: `commands/refactor-mart.md`
- Delete: `commands/commit-push-pr.md`

### Workstream C: Planner Command

- Create: `commands/migrate-mart-plan.md`
- Modify: `commands/status.md` only if it needs a cross-reference to the new planner next action.

### Workstream D: Coordinator Command

- Create: `commands/migrate-mart.md`

### Workstream E: Evals, Docs, And Repo Map

- Modify: `repo-map.json`
- Modify: `docs/wiki/Home.md`
- Modify: `docs/wiki/Command-Reference.md`
- Modify: `docs/wiki/Git-Workflow.md`
- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `tests/evals/prompts/cmd-scope.txt`
- Modify: `tests/evals/prompts/cmd-profile.txt`
- Modify: `tests/evals/prompts/cmd-generate-tests.txt`
- Modify: `tests/evals/prompts/cmd-refactor.txt`
- Modify: `tests/evals/prompts/cmd-generate-model.txt`
- Modify: `tests/evals/prompts/cmd-refactor-mart-stg.txt`
- Modify: `tests/evals/prompts/cmd-refactor-mart-int.txt`
- Create: `tests/evals/prompts/cmd-migrate-mart-plan.txt`
- Create: `tests/evals/prompts/cmd-migrate-mart.txt`
- Modify: `tests/evals/packages/cmd-scope/cmd-scope.yaml`
- Modify: `tests/evals/packages/cmd-profile/cmd-profile.yaml`
- Modify: `tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml`
- Modify: `tests/evals/packages/cmd-refactor/cmd-refactor.yaml`
- Modify: `tests/evals/packages/cmd-generate-model/cmd-generate-model.yaml`
- Modify: `tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml`
- Create: `tests/evals/packages/cmd-migrate-mart-plan/cmd-migrate-mart-plan.yaml`
- Create: `tests/evals/packages/cmd-migrate-mart/cmd-migrate-mart.yaml`
- Modify: `tests/evals/assertions/check-command-summary.js`
- Create: `tests/evals/assertions/check-migrate-mart-plan.js`
- Create: `tests/evals/assertions/check-migrate-mart-resume.js`
- Modify: `tests/unit/repo_structure/test_python_package_layout.py`
- Modify: `tests/unit/repo_structure/test_wiki_public_contract.py`

---

## Task A1: Shared Worktree Helper

**Files:**

- Create: `scripts/stage-worktree.sh`
- Create: `scripts/README.md`
- Modify: `tests/unit/worktree_script/test_worktree_script.py`
- Modify: `tests/unit/repo_structure/test_root_plugin_layout.py`

- [x] **Step 1: Update failing tests for the new shared worktree contract**

  Change `tests/unit/worktree_script/test_worktree_script.py` so `SCRIPT_PATH` targets the new plugin shared script:

  ```python
  SCRIPT_PATH = REPO_ROOT / "shared" / "scripts" / "worktree.sh"
  ```

  Update the creation test to call the new three-argument contract:

  ```python
  result = subprocess.run(
      [str(SCRIPT_PATH), "feature/migrate-mart/040-profile", "040-profile", "feature/migrate-mart"],
      cwd=REPO_ROOT,
      capture_output=True,
      text=True,
      env=env,
      check=False,
  )
  ```

  Assert the new branch is created from the explicit base branch:

  ```python
  assert f"git worktree add -b feature/migrate-mart/040-profile {expected_path} feature/migrate-mart" in log_path.read_text(encoding="utf-8")
  ```

  Add a test that a branch checked out in another worktree returns `status == "ready"` and `reused == true` instead of failing:

  ```python
  payload = json.loads(result.stdout.strip().splitlines()[-1])
  assert payload["status"] == "ready"
  assert payload["branch"] == "feature/migrate-mart/040-profile"
  assert payload["worktree_name"] == "040-profile"
  assert payload["worktree_path"] == str(existing_path)
  assert payload["reused"] is True
  ```

- [x] **Step 2: Run the focused tests and verify they fail**

  ```bash
  cd lib && uv run pytest ../tests/unit/worktree_script/test_worktree_script.py -v
  ```

  Expected: failures because `scripts/stage-worktree.sh` does not exist and the old helper accepts only one argument.

- [x] **Step 3: Implement `scripts/stage-worktree.sh`**

  Implement the plugin runtime worktree helper with an explicit three-argument public contract:

  ```bash
  if [[ $# -ne 3 ]]; then
    echo '{"code":"USAGE","message":"Usage: stage-worktree.sh <branch> <worktree-name> <base-branch>"}' >&2
    exit 2
  fi

  branch="$1"
  worktree_name="$2"
  base_branch="$3"
  repo_root="$(git rev-parse --show-toplevel)"
  worktree_base="${WORKTREE_BASE_DIR:-$repo_root/../worktrees}"
  worktree_path="$worktree_base/$branch"
  ```

  Creation behavior:

  ```bash
  if branch_exists "$branch"; then
    git worktree add "$worktree_path" "$branch"
  else
    git show-ref --verify --quiet "refs/heads/$base_branch" || json_error ...
    git worktree add -b "$branch" "$worktree_path" "$base_branch"
  fi
  ```

  Success output must end with JSON:

  ```json
  {"status":"ready","branch":"<branch>","base_branch":"<base-branch>","worktree_name":"<worktree-name>","worktree_path":"<path>","reused":false}
  ```

  Preserve bootstrap behavior from the current helper: `.env` symlink, optional `direnv allow`, `uv sync --extra dev`, `pyodbc/oracledb` verification, and eval npm bootstrap.

- [x] **Step 4: Document shared script behavior**

  Create `scripts/README.md` with the customer-facing helper contracts:

  ```md
  # Shared Plugin Scripts

  These scripts are invoked by plugin slash commands from the customer project root.

  - `worktree.sh <branch> <worktree-name> <base-branch>`
  - `stage-pr.sh <branch> <base-branch> <title> <body-file>`
  - `stage-pr-merge.sh <pr-number-or-url> <base-branch>`
  - `stage-cleanup.sh <branch> <worktree-path>`
  ```

- [x] **Step 5: Add repo-structure test coverage**

  In `tests/unit/repo_structure/test_root_plugin_layout.py`, assert these paths exist:

  ```python
  assert (repo_root / "shared" / "scripts" / "worktree.sh").exists()
  assert (repo_root / "shared" / "scripts" / "README.md").exists()
  ```

- [x] **Step 6: Run focused tests and verify they pass**

  ```bash
  cd lib && uv run pytest ../tests/unit/worktree_script/test_worktree_script.py ../tests/unit/repo_structure/test_root_plugin_layout.py -v
  ```

- [x] **Step 7: Commit Workstream A checkpoint**

  ```bash
  git add scripts/stage-worktree.sh scripts/README.md tests/unit/worktree_script/test_worktree_script.py tests/unit/repo_structure/test_root_plugin_layout.py
  git commit -m "feat: add deterministic plugin worktree helper"
  ```

## Task A2: Shared PR, Merge, And Cleanup Helpers

**Files:**

- Create: `scripts/stage-pr.sh`
- Create: `scripts/stage-pr-merge.sh`
- Create: `scripts/stage-cleanup.sh`
- Modify: `scripts/README.md`
- Create: `tests/unit/worktree_script/test_stage_pr_script.py`
- Create: `tests/unit/worktree_script/test_stage_pr_merge_script.py`
- Create: `tests/unit/worktree_script/test_stage_cleanup_script.py`
- Modify: `tests/unit/repo_structure/test_root_plugin_layout.py`

- [x] **Step 1: Write failing tests for `stage-pr.sh`**

  Create `tests/unit/worktree_script/test_stage_pr_script.py` using the existing shell-shim style. Cover:

  ```python
  def test_stage_pr_opens_new_pr_against_explicit_base(tmp_path: Path) -> None:
      result = subprocess.run(
          [str(SCRIPT_PATH), "feature/mart/040-profile", "feature/mart", "profile: mart", str(body_file)],
          cwd=REPO_ROOT,
          capture_output=True,
          text=True,
          env=env,
          check=False,
      )
      assert result.returncode == 0
      payload = json.loads(result.stdout)
      assert payload["status"] == "open"
      assert payload["base_branch"] == "feature/mart"
      assert payload["branch"] == "feature/mart/040-profile"
      assert payload["pr_url"] == "https://github.example/pull/42"
  ```

  The fake `gh` shim should record `gh pr list --head`, `gh pr create --base`, and `gh pr edit` calls.

- [x] **Step 2: Write failing tests for `stage-pr-merge.sh`**

  Create `tests/unit/worktree_script/test_stage_pr_merge_script.py`. Cover:

  ```python
  def test_stage_pr_merge_reports_checks_pending(tmp_path: Path) -> None:
      env["FAKE_GH_PR_VIEW_STATE"] = "OPEN"
      env["FAKE_GH_CHECK_STATUS"] = "PENDING"
      result = subprocess.run([...], ...)
      payload = json.loads(result.stdout)
      assert payload["status"] == "checks_pending"
      assert result.returncode == 0
  ```

  Also cover a successful merge:

  ```python
  assert payload["status"] in {"merged", "already_merged"}
  ```

- [x] **Step 3: Write failing tests for `stage-cleanup.sh`**

  Create `tests/unit/worktree_script/test_stage_cleanup_script.py`. Cover idempotent cleanup:

  ```python
  def test_stage_cleanup_removes_worktree_and_branches(tmp_path: Path) -> None:
      result = subprocess.run([str(SCRIPT_PATH), "feature/mart/040-profile", str(worktree_path)], ...)
      payload = json.loads(result.stdout)
      assert payload["status"] == "cleaned"
      assert payload["branch"] == "feature/mart/040-profile"
  ```

- [x] **Step 4: Run tests and verify they fail**

  ```bash
  cd lib && uv run pytest ../tests/unit/worktree_script/test_stage_pr_script.py ../tests/unit/worktree_script/test_stage_pr_merge_script.py ../tests/unit/worktree_script/test_stage_cleanup_script.py -v
  ```

  Expected: failures because the scripts do not exist.

- [x] **Step 5: Implement `stage-pr.sh`**

  Contract:

  ```bash
  stage-pr.sh <branch> <base-branch> <title> <body-file>
  ```

  Required behavior:

  ```bash
  git push -u origin "$branch"
  existing="$(gh pr list --head "$branch" --state open --json number,url --jq '.[0]')"
  if [[ "$existing" == "null" || -z "$existing" ]]; then
    url="$(gh pr create --base "$base_branch" --head "$branch" --title "$title" --body-file "$body_file")"
  else
    gh pr edit "$number" --title "$title" --body-file "$body_file"
  fi
  ```

  Output JSON must include `status`, `branch`, `base_branch`, `pr_number`, and `pr_url`.

- [x] **Step 6: Implement `stage-pr-merge.sh`**

  Contract:

  ```bash
  stage-pr-merge.sh <pr-number-or-url> <base-branch>
  ```

  Required behavior:

  - Return `already_merged` when `gh pr view` reports merged.
  - Return `checks_pending` when required checks are pending.
  - Return `checks_failed` when checks failed.
  - Return `merge_conflict` when GitHub says the PR is not mergeable.
  - Merge with a normal non-force strategy when mergeable.

- [x] **Step 7: Implement `stage-cleanup.sh`**

  Contract:

  ```bash
  stage-cleanup.sh <branch> <worktree-path>
  ```

  Required behavior:

  - Remove the worktree when it exists.
  - Delete the local branch when it exists and is fully merged.
  - Delete the remote branch when it exists.
  - Return `cleaned` or `already_clean` JSON.

- [x] **Step 8: Update repo-structure assertions**

  In `tests/unit/repo_structure/test_root_plugin_layout.py`, assert all shared helper scripts exist and are executable.

- [x] **Step 9: Run focused tests and verify they pass**

  ```bash
  cd lib && uv run pytest ../tests/unit/worktree_script ../tests/unit/repo_structure/test_root_plugin_layout.py -v
  ```

- [x] **Step 10: Commit Workstream A checkpoint**

  ```bash
  git add scripts/stage-pr.sh scripts/stage-pr-merge.sh scripts/stage-cleanup.sh scripts/README.md tests/unit/worktree_script/test_stage_pr_script.py tests/unit/worktree_script/test_stage_pr_merge_script.py tests/unit/worktree_script/test_stage_cleanup_script.py tests/unit/repo_structure/test_root_plugin_layout.py
  git commit -m "feat: add deterministic plugin PR helpers"
  ```

## Task B1: Shared Stage Command Contract

**Files:**

- Modify: `commands/scope-tables.md`
- Modify: `commands/profile-tables.md`
- Modify: `commands/generate-tests.md`
- Modify: `commands/refactor-query.md`
- Modify: `commands/generate-model.md`
- Modify: `commands/refactor-mart.md`

- [x] **Step 1: Add common coordinator-mode parsing text to each stage command**

  Insert an `Arguments` section near the top of each command:

  ````md
  ## Arguments

  Manual mode:

  ```text
  /<command> <object> [object ...]
  ```

  Coordinator mode:

  ```text
  /<command> <plan-file> <stage-id> <worktree-name> <base-branch> <object> [object ...]
  ```

  Treat the invocation as coordinator mode only when `$0` is a Markdown plan path.
  In coordinator mode, read the stage section from `$0`, use `$1` as the numeric stage ID,
  `$2` as the worktree name, `$3` as the PR base branch, and `$4...` as object arguments.
  ````

  For `commands/refactor-mart.md`, keep manual mode as `<plan-file> stg|int`, and add coordinator mode as:

  ```text
  /refactor-mart <migrate-mart-plan-file> <stage-id> <worktree-name> <base-branch> <refactor-mart-plan-file> stg|int
  ```

- [x] **Step 2: Replace legacy worktree setup text**

  In every stage command, use deterministic helper instructions:

  ````md
  In coordinator mode, read `Branch:` from the matching stage section, then run:

  ```bash
  "${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" "<branch>" "<worktree-name>" "<base-branch>"
  ```

  Use the returned `worktree_path` for all reads, writes, commits, and sub-agent prompts.
  ````

  Manual mode should derive a stable branch name from the existing run slug, detect the remote default branch, and call the same helper with that default branch as base.

- [x] **Step 3: Remove human PR prompts**

  In each command summary section, remove:

  ```text
  Raise a PR for this run? (y/n)
  ```

  Replace it with:

  ````md
  After successful item work is committed and pushed, always open or update a PR:

  ```bash
  "${CLAUDE_PLUGIN_ROOT}/scripts/stage-pr.sh" "<branch>" "<base-branch>" "<title>" ".migration-runs/pr-body.<run_id>.md"
  ```

  Report the PR number and URL. In manual mode, tell the human to review and merge the PR. In coordinator mode, return the PR metadata to the coordinator and do not ask any question.
  ````

- [x] **Step 4: Add plan update ownership text**

  In coordinator mode, each command owns only its stage section in the Markdown plan while it runs:

  ```md
  After each stage substep or item result, update only the matching `## Stage <stage-id>` checklist in `<plan-file>`, then commit the plan update with the artifact or catalog change that caused it.
  ```

- [x] **Step 5: Run markdownlint for command docs**

  ```bash
  markdownlint commands/scope-tables.md commands/profile-tables.md commands/generate-tests.md commands/refactor-query.md commands/generate-model.md commands/refactor-mart.md
  ```

- [x] **Step 6: Commit Workstream B checkpoint**

  ```bash
  git add commands/scope-tables.md commands/profile-tables.md commands/generate-tests.md commands/refactor-query.md commands/generate-model.md commands/refactor-mart.md
  git commit -m "feat: make stage commands coordinator-aware"
  ```

## Task B2: Remove Legacy Commit-Push-PR Command

**Files:**

- Delete: `commands/commit-push-pr.md`

- [x] **Step 1: Delete legacy PR command**

  Delete `commands/commit-push-pr.md`. Coordinator-aware stage commands call `scripts/stage-pr.sh` directly, and manual stage runs use the same deterministic helper from the stage command.

- [x] **Step 2: Commit Workstream B checkpoint**

  ```bash
  git add commands/commit-push-pr.md
  git commit -m "chore: remove legacy commit-push-pr command"
  ```

## Task C1: Migrate Mart Plan Command

**Files:**

- Create: `commands/migrate-mart-plan.md`
- Modify: `tests/unit/repo_structure/test_python_package_layout.py`

- [x] **Step 1: Add repo-structure failing test for new command**

  In `tests/unit/repo_structure/test_python_package_layout.py`, add `commands/migrate-mart-plan.md` to the command file expectations.

- [x] **Step 2: Run focused test and verify failure**

  ```bash
  cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -v
  ```

  Expected: failure because `commands/migrate-mart-plan.md` does not exist.

- [x] **Step 3: Create command frontmatter**

  Create `commands/migrate-mart-plan.md`:

  ```md
  ---
  name: migrate-mart-plan
  description: Whole-scope mart migration planner. Validates readiness, scopes when needed, enforces catalog ownership resolution, and writes a resumable Markdown operational plan.
  user-invocable: true
  argument-hint: "<slug>"
  ---
  ```

- [x] **Step 4: Add command guards**

  Required guard section:

  ```md
  ## Guards

  - `$0` must be a lowercase hyphen-separated slug. If missing, fail with `SLUG_REQUIRED`.
  - `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
  - `runtime.source`, `runtime.target`, and `runtime.sandbox` must be present in `manifest.json`.
  - Source, target, and sandbox must be reachable through existing CLI checks.
  - `dbt/dbt_project.yml` must exist before writing an executable plan.
  - If catalog ownership is unresolved after scoping, stop before writing an executable plan and tell the human which `ad-migration add-source-table`, `ad-migration add-seed-table`, or `ad-migration exclude-table` decisions are needed.
  ```

- [x] **Step 5: Add planner pipeline**

  Include these exact steps:

  1. Detect default branch.
  2. Create/reuse coordinator branch `feature/migrate-mart-<slug>` using `scripts/stage-worktree.sh`.
  3. Run fresh `migrate-util batch-plan`.
  4. If `scope_phase` has objects, run `/scope-tables` in coordinator mode as Stage 020, merge the returned PR, clean up the stage worktree, refresh coordinator branch, and rerun `batch-plan`.
  5. If source/seed/exclude decisions are unresolved, report required CLI decisions and stop.
  6. Write `docs/migration-plans/<slug>/README.md`.
  7. Commit the plan on the coordinator branch and open/update the final coordinator PR only when explicitly instructed by `/migrate-mart`, not during planning.

- [x] **Step 6: Add Markdown plan template**

  The command must write sections:

  ```md
  ## Coordinator
  ## Source Replication
  ## Stage 010: Runtime Readiness
  ## Stage 020: Scope
  ## Stage 030: Catalog Ownership Check
  ## Stage 040: Profile
  ## Stage 050: Setup Target
  ## Stage 060: Setup Sandbox
  ## Stage 070: Generate Tests
  ## Stage 080: Refactor Query
  ## Stage 090: Replicate Source Tables
  ## Stage 100: Generate Model
  ## Stage 110: Refactor Mart Staging
  ## Stage 120: Refactor Mart Higher
  ## Stage 130: Final Status
  ```

  Each executable stage must include `Agent`, `Slash command`, `Invocation`, `Branch`, `Base branch`, `Worktree name`, `Worktree path`, `PR`, and `Status`.

  Source replication defaults:

  ```md
  - Row limit: 10000
  - Command: `ad-migration replicate-source-tables --limit 10000 --yes`
  ```

- [x] **Step 7: Run markdownlint and repo-structure test**

  ```bash
  markdownlint commands/migrate-mart-plan.md
  cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -v
  ```

- [x] **Step 8: Commit Workstream C checkpoint**

  ```bash
  git add commands/migrate-mart-plan.md tests/unit/repo_structure/test_python_package_layout.py
  git commit -m "feat: add migrate-mart-plan command"
  ```

## Task D1: Migrate Mart Coordinator Command

**Files:**

- Create: `commands/migrate-mart.md`
- Modify: `tests/unit/repo_structure/test_python_package_layout.py`

- [x] **Step 1: Add repo-structure failing test**

  Add `commands/migrate-mart.md` to command file expectations in `tests/unit/repo_structure/test_python_package_layout.py`.

- [x] **Step 2: Run focused test and verify failure**

  ```bash
  cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -v
  ```

- [x] **Step 3: Create command frontmatter**

  ```md
  ---
  name: migrate-mart
  description: Execute a migrate-mart Markdown plan by resuming the first incomplete task, launching one stage subagent at a time, merging stage PRs, and updating the coordinator plan.
  user-invocable: true
  argument-hint: "<plan-file>"
  ---
  ```

- [x] **Step 4: Add coordinator guards**

  Required guard section:

  ```md
  ## Guards

  - `$0` must be an existing Markdown plan file. If missing, fail with `PLAN_REQUIRED`.
  - The plan must include `## Coordinator`.
  - The coordinator section must include Branch, Worktree name, Worktree path, Base branch, and Status.
  - Every incomplete executable stage must include Invocation, Branch, Base branch, Worktree name, Worktree path, PR, and Status.
  - If plan metadata is malformed or missing, mark the coordinator blocked with `PLAN_INVALID` and stop.
  ```

- [x] **Step 5: Add resume algorithm**

  Include exact behavior:

  1. Attach to the coordinator worktree using the plan's coordinator branch, worktree name, and base branch.
  2. Scan stage sections in numeric order.
  3. Pick the first stage with `Status` not in `complete`, `skipped`, or `superseded`.
  4. Reconcile git/PR state:
     - existing stage worktree with incomplete work: relaunch recorded invocation
     - stage branch commits without PR: call `stage-pr.sh`
     - open PR: call `stage-pr-merge.sh`
     - already merged PR: mark merge complete
     - merged stage with remaining worktree: call `stage-cleanup.sh`
  5. After each merge, refresh coordinator worktree, rerun `migrate-util batch-plan`, update the Markdown plan, and commit the plan update.
  6. Launch exactly one subagent at a time.

- [x] **Step 6: Add stage execution table**

  The command should include this mapping:

  ```md
  | Stage | Invocation source |
  |---|---|
  | 040 | recorded `/profile-tables ...` invocation |
  | 050 | deterministic `ad-migration setup-target` stage subagent |
  | 060 | deterministic `ad-migration setup-sandbox --yes` stage subagent |
  | 070 | recorded `/generate-tests ...` invocation |
  | 080 | recorded `/refactor-query ...` invocation |
  | 090 | recorded `ad-migration replicate-source-tables --limit <plan-limit> --yes` invocation |
  | 100 | recorded `/generate-model ...` invocation |
  | 110 | recorded `/refactor-mart ... stg` invocation |
  | 120 | recorded `/refactor-mart ... int` invocation |
  ```

- [x] **Step 7: Add final PR behavior**

  State:

  ```md
  When all stages are complete, open or update the final coordinator PR from the coordinator branch to the remote default branch. Do not merge the final coordinator PR. Report the URL for human review.
  ```

- [x] **Step 8: Run markdownlint and repo-structure test**

  ```bash
  markdownlint commands/migrate-mart.md
  cd lib && uv run pytest ../tests/unit/repo_structure/test_python_package_layout.py -v
  ```

- [x] **Step 9: Commit Workstream D checkpoint**

  ```bash
  git add commands/migrate-mart.md tests/unit/repo_structure/test_python_package_layout.py
  git commit -m "feat: add migrate-mart coordinator command"
  ```

## Task E1: Promptfoo Evals For Stage PR Contract

**Files:**

- Modify: `tests/evals/prompts/cmd-scope.txt`
- Modify: `tests/evals/prompts/cmd-profile.txt`
- Modify: `tests/evals/prompts/cmd-generate-tests.txt`
- Modify: `tests/evals/prompts/cmd-refactor.txt`
- Modify: `tests/evals/prompts/cmd-generate-model.txt`
- Modify: `tests/evals/prompts/cmd-refactor-mart-stg.txt`
- Modify: `tests/evals/prompts/cmd-refactor-mart-int.txt`
- Modify: `tests/evals/assertions/check-command-summary.js`
- Modify: all corresponding `tests/evals/packages/cmd-*/` YAML files.

- [x] **Step 1: Update eval prompts**

  Existing eval prompts tell agents not to use git/worktree/PR commands. Preserve that for fixture safety, but add an assertion target that the command spec contains automatic PR handoff language.

  Add to each prompt:

  ```text
  In this eval fixture, do not actually run git, scripts/stage-* helpers, gh, or cleanup commands. Instead, verify the command spec would open/update a PR automatically in a real project and report that handoff in the final summary.
  ```

- [x] **Step 2: Extend `check-command-summary.js`**

  Add optional expected terms:

  ```javascript
  const expectedPrTerms = parseTerms(context.vars.expected_pr_terms || '');
  for (const term of expectedPrTerms) {
    if (!output.toLowerCase().includes(term.toLowerCase())) {
      return fail(`Missing expected PR handoff term: ${term}`);
    }
  }
  ```

- [x] **Step 3: Update package YAML variables**

  Add `expected_pr_terms` to one smoke case per command:

  ```yaml
  expected_pr_terms: "PR,Branch,Worktree"
  ```

- [x] **Step 4: Run focused evals**

  ```bash
  cd tests/evals && npm run eval:cmd-scope
  cd tests/evals && npm run eval:cmd-profile
  cd tests/evals && npm run eval:cmd-generate-tests
  cd tests/evals && npm run eval:cmd-refactor
  cd tests/evals && npm run eval:cmd-generate-model
  cd tests/evals && npm run eval:cmd-refactor-mart
  ```

- [x] **Step 5: Commit Workstream E checkpoint**

  ```bash
  git add tests/evals/prompts/cmd-scope.txt tests/evals/prompts/cmd-profile.txt tests/evals/prompts/cmd-generate-tests.txt tests/evals/prompts/cmd-refactor.txt tests/evals/prompts/cmd-generate-model.txt tests/evals/prompts/cmd-refactor-mart-stg.txt tests/evals/prompts/cmd-refactor-mart-int.txt tests/evals/assertions/check-command-summary.js tests/evals/packages/cmd-scope/cmd-scope.yaml tests/evals/packages/cmd-profile/cmd-profile.yaml tests/evals/packages/cmd-generate-tests/cmd-generate-tests.yaml tests/evals/packages/cmd-refactor/cmd-refactor.yaml tests/evals/packages/cmd-generate-model/cmd-generate-model.yaml tests/evals/packages/cmd-refactor-mart/cmd-refactor-mart.yaml
  git commit -m "test: update command evals for automatic PR handoff"
  ```

## Task E2: Promptfoo Evals For Migrate Mart Commands

**Files:**

- Create: `tests/evals/prompts/cmd-migrate-mart-plan.txt`
- Create: `tests/evals/prompts/cmd-migrate-mart.txt`
- Create: `tests/evals/packages/cmd-migrate-mart-plan/cmd-migrate-mart-plan.yaml`
- Create: `tests/evals/packages/cmd-migrate-mart/cmd-migrate-mart.yaml`
- Create: `tests/evals/assertions/check-migrate-mart-plan.js`
- Create: `tests/evals/assertions/check-migrate-mart-resume.js`
- Modify: `tests/evals/package.json`

- [ ] **Step 1: Add plan assertion**

  `check-migrate-mart-plan.js` should verify output or written plan contains:

  ```javascript
  const requiredSections = [
    '## Coordinator',
    '## Source Replication',
    '## Stage 010: Runtime Readiness',
    '## Stage 020: Scope',
    '## Stage 040: Profile',
    '## Stage 130: Final Status',
  ];
  ```

  Also verify `Row limit: 10000`, `Worktree name:`, `Base branch:`, and `Invocation:`.

- [ ] **Step 2: Add coordinator resume assertion**

  `check-migrate-mart-resume.js` should verify the command chooses the first incomplete stage and does not accept or mention a start-stage argument.

- [ ] **Step 3: Add prompt files**

  `tests/evals/prompts/cmd-migrate-mart-plan.txt`:

  ```text
  Run /migrate-mart-plan for {{plan_slug}} in {{run_path}}. Do not run real git, gh, or scripts commands in this eval. Validate guards, render the intended plan, and report blocker state when applicable.
  ```

  `tests/evals/prompts/cmd-migrate-mart.txt`:

  ```text
  Run /migrate-mart {{plan_file}} in {{run_path}}. Do not run real git, gh, or scripts commands in this eval. Validate the plan, identify the first incomplete stage, and report the intended next action.
  ```

- [ ] **Step 4: Add package YAMLs**

  Add scenarios:

  - missing manifest guard
  - missing runtime target guard
  - missing sandbox guard
  - catalog ownership blocked
  - happy path plan shape
  - malformed plan blocks coordinator
  - first incomplete stage resume

- [ ] **Step 5: Add package scripts**

  In `tests/evals/package.json`, add:

  ```json
  "eval:cmd-migrate-mart-plan": "promptfoo eval -c packages/cmd-migrate-mart-plan/cmd-migrate-mart-plan.yaml",
  "eval:cmd-migrate-mart": "promptfoo eval -c packages/cmd-migrate-mart/cmd-migrate-mart.yaml"
  ```

- [ ] **Step 6: Run focused evals**

  ```bash
  cd tests/evals && npm run eval:cmd-migrate-mart-plan
  cd tests/evals && npm run eval:cmd-migrate-mart
  ```

- [ ] **Step 7: Commit Workstream E checkpoint**

  ```bash
  git add tests/evals/prompts/cmd-migrate-mart-plan.txt tests/evals/prompts/cmd-migrate-mart.txt tests/evals/packages/cmd-migrate-mart-plan/cmd-migrate-mart-plan.yaml tests/evals/packages/cmd-migrate-mart/cmd-migrate-mart.yaml tests/evals/assertions/check-migrate-mart-plan.js tests/evals/assertions/check-migrate-mart-resume.js tests/evals/package.json
  git commit -m "test: add migrate-mart command evals"
  ```

## Task E3: Public Docs And Repo Map

**Files:**

- Modify: `repo-map.json`
- Modify: `docs/wiki/Home.md`
- Modify: `docs/wiki/Command-Reference.md`
- Modify: `docs/wiki/Git-Workflow.md`
- Modify: `docs/wiki/Installation-and-Prerequisites.md`
- Modify: `tests/unit/repo_structure/test_python_package_layout.py`
- Modify: `tests/unit/repo_structure/test_wiki_public_contract.py`

- [ ] **Step 1: Update repo-map**

  Add `scripts/` to key directories and mention:

  ```json
  "scripts/": "Plugin-owned deterministic helpers for customer-project worktrees, stage PRs, PR merges, and stage cleanup."
  ```

  Add new command entries:

  ```json
  "migrate_mart_plan_command": "/migrate-mart-plan <slug>",
  "migrate_mart_command": "/migrate-mart <plan-file>"
  ```

- [ ] **Step 2: Update wiki command list**

  In `docs/wiki/Home.md` and `docs/wiki/Command-Reference.md`, add `/migrate-mart-plan` and `/migrate-mart` as the whole-scope mart workflow.

  Keep internal helper paths out of public prose.

- [ ] **Step 3: Update Git workflow wiki**

  In `docs/wiki/Git-Workflow.md`, explain that stage commands now create/update PRs automatically and that `/migrate-mart` merges stage PRs into the coordinator branch, while final PR remains human-reviewed.

- [ ] **Step 4: Update installation/prereq docs**

  In `docs/wiki/Installation-and-Prerequisites.md`, update the batch command language that currently says commands ask before opening PRs.

- [ ] **Step 5: Update structure tests**

  Add assertions that:

  - `repo-map.json` mentions `scripts/`
  - public wiki mentions `/migrate-mart-plan` and `/migrate-mart`
  - public wiki does not expose `scripts/` paths as user commands

- [ ] **Step 6: Run docs tests and markdownlint**

  ```bash
  cd lib && uv run pytest ../tests/unit/repo_structure -v
  markdownlint docs/wiki/Home.md docs/wiki/Command-Reference.md docs/wiki/Git-Workflow.md docs/wiki/Installation-and-Prerequisites.md repo-map.json
  ```

- [ ] **Step 7: Commit Workstream E checkpoint**

  ```bash
  git add repo-map.json docs/wiki/Home.md docs/wiki/Command-Reference.md docs/wiki/Git-Workflow.md docs/wiki/Installation-and-Prerequisites.md tests/unit/repo_structure/test_python_package_layout.py tests/unit/repo_structure/test_wiki_public_contract.py
  git commit -m "docs: document migrate-mart workflow"
  ```

## Final Integration And Verification

- [ ] **Step 1: Integrate workstreams in order**

  Merge workstream branches into the integration branch in this order:

  ```text
  feature/migrate-mart-shared-scripts
  feature/migrate-mart-stage-commands
  feature/migrate-mart-planner
  feature/migrate-mart-coordinator
  feature/migrate-mart-evals-docs
  ```

- [ ] **Step 2: Run focused unit tests**

  ```bash
  cd lib && uv run pytest ../tests/unit/worktree_script ../tests/unit/repo_structure -v
  ```

- [ ] **Step 3: Run command evals**

  ```bash
  cd tests/evals && npm run eval:cmd-scope
  cd tests/evals && npm run eval:cmd-profile
  cd tests/evals && npm run eval:cmd-generate-tests
  cd tests/evals && npm run eval:cmd-refactor
  cd tests/evals && npm run eval:cmd-generate-model
  cd tests/evals && npm run eval:cmd-refactor-mart
  cd tests/evals && npm run eval:cmd-migrate-mart-plan
  cd tests/evals && npm run eval:cmd-migrate-mart
  ```

- [ ] **Step 4: Run smoke eval**

  ```bash
  cd tests/evals && npm run eval:smoke
  ```

- [ ] **Step 5: Run markdownlint**

  ```bash
  markdownlint commands/*.md docs/design/migrate-mart-coordinator/README.md docs/superpowers/plans/2026-04-19-migrate-mart-coordinator.md docs/wiki/Home.md docs/wiki/Command-Reference.md docs/wiki/Git-Workflow.md docs/wiki/Installation-and-Prerequisites.md
  ```

- [ ] **Step 6: Final subagent review**

  Dispatch a final reviewer with:

  ```text
  Review the implementation against docs/design/migrate-mart-coordinator/README.md and docs/superpowers/plans/2026-04-19-migrate-mart-coordinator.md. Focus on missing plan requirements, unsafe git/PR behavior, resume gaps, and command/eval drift. Return findings by severity with file references.
  ```

- [ ] **Step 7: Prepare development branch completion**

  Use `finishing-a-development-branch` after all tests and reviews pass.
