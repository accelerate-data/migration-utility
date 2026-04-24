# Migration Utility

Claude Code plugin + batch CLI pipeline that migrates warehouse stored procedures to dbt models on Vibedata's platform. Targets silver and gold transformations only (bronze is out of scope).

**Maintenance rule:** This file contains architecture, conventions, and guidelines — not product details. Do not add counts, feature descriptions, or any fact that can be discovered by reading code. If it will go stale when the code changes, it doesn't belong here — point to the source file instead.

## Instruction Hierarchy

Use this precedence when maintaining agent guidance:

1. `AGENTS.md` (canonical, cross-agent source of truth)
2. `.claude/rules/*.md` (shared detailed rules; agent-agnostic content)
3. `.claude/skills/*/SKILL.md` (workflow playbooks)
4. Agent-specific adapter files (for example `CLAUDE.md`) that reference canonical docs

Adapter files must not duplicate canonical policy unless they are adding agent-specific behavior.

## Architecture

| Layer | Technology |
|---|---|
| Agent runtime | Claude Code CLI (`claude --plugin-dir . --agent <name>`) |
| Codex plugin runtime | `.codex-plugin/plugin.json` exposes root `skills/` and the local DDL MCP server |
| MCP server | genai-toolbox (HTTP mode on GH Actions, stdio locally) |
| MCP server (Oracle) | SQLcl `-mcp` (stdio, local only) |
| Runtime | GitHub Actions (headless execution) |

**Source scope:** SQL-based warehouse stored procedures. Non-SQL runtimes are post-MVP.

**Agent startup context:** Read `repo-map.json` before starting any non-trivial task — it has structure, entrypoints, modules, and commands.

## Repository Structure

**Key directories, modules, entrypoints, and commands:** See `repo-map.json`. Read it before exploring code — skip repo-wide rediscovery if it covers the task.

## Dev Commands

See `repo-map.json` → `commands` for the full command reference.

## Testing

### When to write tests

1. New Python module or function with testable logic → pytest tests
2. New agent skill or command → integration test
3. Bug fix → regression test

Purely cosmetic changes or simple wiring don't require tests. If unclear, ask the user.

### Test discipline

Before writing any test code, read existing tests for the files you changed:

1. Update tests that broke due to your changes
2. Remove tests that are now redundant
3. Add new tests only for genuinely new behavior
4. Never add tests just to increase count — every test must catch a real regression

### Choosing which tests to run

Determine what you changed, then pick the right runner:

| What changed | Tests to run |
|---|---|
| Python shared library | `cd lib && uv run pytest` |
| Python integration (Docker SQL Server) | `cd lib && uv run pytest -m integration` |
| Python integration (Docker Oracle) | `cd lib && uv run pytest -m oracle` |
| MCP server | `cd mcp/ddl && uv run pytest` |
| Unsure | all of the above |

When a change depends on local infrastructure (for example SQL Server-backed ignored tests), document in the PR which commands were run and which were not run.

**Stale venv after a repo move:** If `mcp/ddl` tests fail with `cannot execute: No such file or directory`, the `.venv` has a stale interpreter path from a prior directory location. Fix: `rm -rf mcp/ddl/.venv && cd mcp/ddl && uv sync`.

**Worktree venv for integration tests:** Worktrees get a fresh `.venv` on first run. For integration tests (pyodbc, oracledb), sync with the `dev` extra: `cd lib && rm -rf .venv && uv sync --extra dev`.

## Documentation Locations

Use these canonical locations for repository documentation:

- Design docs: `docs/design/`
- Functional specs: `docs/functional/`
- Implementation plans: `docs/plans/`
- Reference docs: `docs/reference/`
- End-user docs: `docs/wiki/`

Design topics get their own subdirectory with a `README.md`
(e.g. `docs/design/orchestrator-design/README.md`). The index at `docs/design/README.md` must be
updated when adding a new design subdirectory.

Functional spec topics get their own subdirectory with a `README.md`. The index at
`docs/functional/README.md` must be updated when adding a new functional spec subdirectory.

If a generic agent workflow or skill asks you to write a document somewhere else, use the matching
canonical location in this repo instead. Update an index only for documentation types that require
one: design docs and functional specs.

Write design docs concisely — state the decision and the reason, not the reasoning process. One
sentence beats a paragraph. Avoid restating what the code already makes obvious.

## Code Style

- Granular commits: one concern per commit, run tests before each
- Stage specific files — use `git add <file>` not `git add .`
- All `.md` files must pass `markdownlint` before committing (`markdownlint <file>`)
- Markdownlint line length (`MD013`) is intentionally disabled. Do not hard-wrap prose solely for line-length concerns; prefer readable paragraphs and only add line breaks for Markdown structure.
- Canonical naming and error-handling conventions live in `.claude/rules/coding-conventions.md`
- Prefer clean-break designs over backward-compatibility shims. Do not engineer for backward compatibility unless the user, issue, or design explicitly requires it.
- When fixing a bug or review comment, also fix adjacent pre-existing defects in the same touched code path when they violate repo policy, break consistency, or would leave the requested change half-correct. Do not use "pre-existing" by itself as a reason to leave a touched policy violation in place.

### Error handling

See `.claude/rules/coding-conventions.md` for canonical error-handling policy.

## Maintenance Rules

| Artifact | Update when |
|---|---|
| `AGENTS.md` | A fact is durable, non-obvious, and won't be obvious from code |
| `repo-map.json` | Any repo-map entry becomes stale: entrypoints, commands, modules, package structure, test/eval layout, plugin/skill layout, or referenced documentation indexes change |

Update stale entries in the same commit that introduced the structural change.

## Issue Management

- **PR title format:** `VU-XXX: short description`
- **PR body link:** `Fixes VU-XXX`
- **Worktrees:** `../worktrees/<branchName>` relative to repo root. Full rules: `.claude/rules/git-workflow.md`.
- **Worktree creation:** Use `./scripts/worktree.sh <branch-name>` as the canonical maintainer workflow for creating or attaching a repo worktree and bootstrapping it. Do not use raw `git worktree add` unless you are debugging the wrapper itself.
- **Worktree script distinction:** `scripts/worktree.sh` is the maintainer development helper. `scripts/stage-worktree.sh` and the other `scripts/stage-*` helpers are plugin runtime helpers used by customer-project slash commands.

## MCP Servers

Only the `ddl` MCP server is bundled. It reads pre-extracted local `.sql` files and is used by skills for DDL analysis. No live-DB MCP servers are included — add your own if needed.

## Plugin Manifests

Claude and Codex plugin versions must move together with the release-facing Python package versions. Update `.claude-plugin/plugin.json`, `.codex-plugin/plugin.json`, the package `pyproject.toml` files, and their `uv.lock` files in the same change, then run `python scripts/validate_plugin_manifests.py` and `python scripts/check_version_consistency.py`.

Codex supports root `skills/` and `.mcp.json` in this repository. Root `commands/` remain Claude-only until a Codex command-runtime contract exists; the durable surface decision lives in `docs/reference/codex-plugin-surface/README.md`.

## Logging

Every new feature must include logging. Canonical logging conventions and log-level guidance live in `.claude/rules/logging-policy.md`.
