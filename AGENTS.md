# Migration Utility

Claude Code plugin + batch CLI pipeline that migrates Microsoft Fabric Warehouse stored procedures to dbt models on Vibedata's platform. Targets silver and gold transformations only (bronze is out of scope).

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
| Agent runtime | Claude Code CLI (`claude --plugin-dir plugin/ --agent <name>`) |
| MCP server | genai-toolbox (HTTP mode on GH Actions, stdio locally) |
| MCP server (Oracle) | SQLcl `-mcp` (stdio, local only) |
| Runtime | GitHub Actions (headless execution) |

**Source scope:** Fabric Warehouse (T-SQL stored procedures via ADF pipelines). Lakehouse/Spark is post-MVP.

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
| Python shared library | `cd plugin/lib && uv run pytest` |
| Python integration (Docker SQL Server) | `cd plugin/lib && uv run pytest -m integration` |
| Python integration (Docker Oracle) | `cd plugin/lib && uv run pytest -m oracle` |
| MCP server | `cd plugin/mcp/ddl && uv run pytest` |
| Unsure | all of the above |

When a change depends on local infrastructure (for example SQL Server-backed ignored tests), document in the PR which commands were run and which were not run.

**Stale venv after a repo move:** If `plugin/mcp/ddl` tests fail with `cannot execute: No such file or directory`, the `.venv` has a stale interpreter path from a prior directory location. Fix: `rm -rf plugin/mcp/ddl/.venv && cd plugin/mcp/ddl && uv sync`.

**Worktree venv for integration tests:** Worktrees get a fresh `.venv` on first run. For integration tests (pyodbc, oracledb), sync with the `dev` extra: `cd plugin/lib && rm -rf .venv && uv sync --extra dev`.

## Design Docs

Design notes live in `docs/design/`. Each topic gets its own subdirectory with a `README.md`
(e.g. `docs/design/orchestrator-design/README.md`). The index at `docs/design/README.md` must be
updated when adding a new subdirectory.

If a generic agent workflow or skill asks you to write a spec or design doc somewhere else, use
`docs/design/<topic>/README.md` in this repo instead and update the design index.

Write design docs concisely — state the decision and the reason, not the reasoning process. One
sentence beats a paragraph. Avoid restating what the code already makes obvious.

## Code Style

- Granular commits: one concern per commit, run tests before each
- Stage specific files — use `git add <file>` not `git add .`
- All `.md` files must pass `markdownlint` before committing (`markdownlint <file>`)
- Canonical naming and error-handling conventions live in `.claude/rules/coding-conventions.md`
- Prefer clean-break designs over backward-compatibility shims. Do not engineer for backward compatibility unless the user, issue, or design explicitly requires it.

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
- **Worktree creation:** Use `./scripts/worktree.sh <branch-name>` as the canonical way to create or attach a worktree and bootstrap it. Do not use raw `git worktree add` unless you are debugging the wrapper itself.

## MCP Servers

### SQL Server (mssql)

Configured in `.mcp.json` via genai-toolbox. Uses `SA_PASSWORD` from `.env`. Tool: `mcp__mssql__mssql-execute-sql`.

### Oracle

Configured in `.mcp.json` via SQLcl `-mcp`. Requires Java 11+ and SQLcl installed locally (`brew install --cask sqlcl`).

The Oracle MCP server does **not** auto-connect on startup. At the beginning of each session, run:

```text
mcp__oracle__run-sqlcl: connect sh/sh@localhost:1521/FREEPDB1
```

After connecting, use `mcp__oracle__run-sql` for queries and `mcp__oracle__schema-information` for metadata.

Setup: see `docs/reference/setup-docker/README.md`.

## Logging

Every new feature must include logging. Canonical logging conventions and log-level guidance live in `.claude/rules/logging-policy.md`.
