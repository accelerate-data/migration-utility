# Command Reference

## Plugin commands

Plugin commands run inside Claude Code and handle LLM-driven pipeline stages.

### Bootstrap

| Command | Purpose |
|---|---|
| `/init-ad-migration` | Install the `ad-migration` CLI, check prerequisites, and scaffold the project |

### Migration pipeline

| Command | Purpose |
|---|---|
| `/scope` | Resolve writers for tables or analyze views |
| `/profile` | Write migration profiles for tables, views, or MVs |
| `/generate-tests` | Generate and review test scenarios, then capture ground truth |
| `/refactor` | Persist proof-backed import/logical/final refactors |
| `/generate-model` | Generate dbt artifacts from approved refactors and tests |
| `/status` | Show current readiness and the next best action |

### Source and scope management

| Command | Purpose |
|---|---|
| `/reset-migration` | Clear one migration stage so it can be re-run cleanly |

## ad-migration CLI

Deterministic setup and pipeline state commands, usable from a terminal or CI without Claude Code.

Install via `/init-ad-migration` (automatic) or manually:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
```

Dev usage (no install needed):

```bash
uv run --project lib ad-migration <command>
```

### Setup commands

| Command | Purpose |
|---|---|
| `ad-migration setup-source` | Extract DDL and build the local catalog from a live source database |
| `ad-migration setup-target` | Scaffold the dbt project and generate `sources.yml` |
| `ad-migration setup-sandbox` | Create the active sandbox execution endpoint |
| `ad-migration teardown-sandbox` | Drop the sandbox endpoint and clear sandbox metadata |

### Pipeline state commands

| Command | Purpose |
|---|---|
| `ad-migration reset` | Clear one migration stage so it can be re-run |
| `ad-migration exclude-table` | Exclude tables or views from the active migration pipeline |
| `ad-migration add-source-table` | Confirm tables as dbt sources (`is_source: true`) |

## Git workflow scripts

Shell scripts for git operations — called by Claude rules, also runnable directly.

| Script | Purpose |
|---|---|
| `scripts/commit.sh` | Stage specific files and commit |
| `scripts/commit-push-pr.sh` | Stage, commit, push, and open a PR |
| `scripts/cleanup-worktrees.sh` | Remove merged worktrees and stale merged branches |

## Notes

- Batch plugin commands use the git checkpoint flow and may create or reuse worktrees.
- Successful items are committed as they complete.
- Source-confirmed tables are skipped by downstream migration commands.
