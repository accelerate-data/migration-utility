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
| [`/migrate-mart-plan`](Command-Migrate-Mart-Plan) | Build or refresh the whole-mart migration plan, open the planning PR, and stop for source, seed, and exclusion review |
| [`/migrate-mart`](Command-Migrate-Mart) | Execute the first incomplete whole-mart plan stage, merging stage PRs into the coordinator branch and leaving the final PR for review |
| `/scope-tables` | Resolve writers for tables or analyze views |
| `/profile-tables` | Write migration profiles for tables, views, or MVs |
| `/generate-tests` | Generate and review test scenarios, then capture ground truth |
| `/refactor-query` | Persist proof-backed import/logical/final refactors |
| `/generate-model` | Generate dbt artifacts from approved refactors and tests |
| `/status` | Show current readiness and the next best action |

### Repository maintenance

| Command | Purpose |
|---|---|
| `/cleanup-worktrees` | Remove merged worktrees and their branches after PR cleanup |

## ad-migration CLI

Deterministic setup and pipeline state commands, usable from a terminal or CI without Claude Code.

Use this page as the one-page command summary. For flags, environment variables, files written, and exit-code behavior, use [[CLI Reference]].

Install via `/init-ad-migration` or manually. On macOS:

```bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
```

On Linux and WSL, install the GitHub release wheel artifacts into Python 3.11+. Native Windows is not supported. Use WSL for the local workflow.

### Setup commands

| Command | Purpose |
|---|---|
| `ad-migration setup-source` | Extract DDL and build the local catalog from a live source database |
| `ad-migration setup-target` | Scaffold the dbt project and generate staging source metadata |
| `ad-migration setup-sandbox` | Create the active sandbox execution endpoint |
| `ad-migration teardown-sandbox` | Drop the sandbox endpoint and clear sandbox metadata |

### Pipeline state commands

| Command | Purpose |
|---|---|
| `ad-migration reset` | Clear one migration stage so it can be re-run |
| `ad-migration exclude-table` | Exclude tables or views from the active migration pipeline |
| `ad-migration add-source-table` | Confirm tables as dbt sources (`is_source: true`) |

## Notes

- Source-confirmed tables are skipped by downstream migration commands.
- The `ad-migration` CLI does not commit, push, open PRs, or clean worktrees; do those git operations yourself in the shell.
- Batch plugin commands commit and push durable progress, then open or update their PRs automatically.
- For the full ad-migration CLI reference including options and exit codes, see [[CLI Reference]].
