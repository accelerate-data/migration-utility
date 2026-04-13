# Migration Utility

A Claude Code plugin that migrates warehouse stored procedures to dbt models. Targets silver and gold transformations from SQL-based sources; non-SQL runtimes are out of scope.

See `AGENTS.md` for architecture, conventions, and agent guidance.

---

## Prerequisites

### Required tools

| Tool | Purpose |
|------|---------|
| Python 3.11+ | Runtime for `lib/` and `mcp/` |
| [uv](https://docs.astral.sh/uv/) | Python package manager |
| Node.js + npm | Promptfoo eval harness for migration-only evals (`tests/evals/`) |
| [Claude Code CLI](https://docs.anthropic.com/claude-code) | Plugin development and agent execution |
| [direnv](https://direnv.net/) | Auto-loads `.env` credentials |
| [markdownlint-cli](https://github.com/igorshubovych/markdownlint-cli) | All `.md` files must pass before commit |

### Claude Code plugins

Install these via the Claude Code plugin marketplace before starting work:

- **promptfoo-evals** — eval harness skill for creating/updating promptfoo suites
- **@claude-plugins-official** — official plugin pack (enables code-simplifier, frontend-design, feature-dev, code-review as configured in `.claude/settings.json`)

### Optional (for integration tests)

| Tool | Purpose |
|------|---------|
| Docker Desktop | Runs SQL Server 2022 container for `pytest -m integration` |
| [gh CLI](https://cli.github.com/) | GitHub API interactions |

### Environment variables

Fill in `.env` (commented examples are included). For local SQL Server MCP usage, set `SA_PASSWORD`; `MSSQL_HOST`, `MSSQL_PORT`, and `MSSQL_DB` default from `.mcp.json` and can be overridden in your environment if needed. Then:

```bash
direnv allow
```

---

## Setup

```bash
git clone https://github.com/accelerate-data/migration-utility
cd migration-utility
uv sync --project lib
```

To run the plugin locally:

```bash
claude --plugin-dir .
```

---

## Repository Structure

```text
.claude/              Agent rules, skills, and memory
agents/               Claude agent definitions
lib/                  Python library (uv project)
  shared/             DDL analysis modules and JSON schemas
mcp/
  ddl/                DDL file MCP server (structured AST access)
  mssql/              genai-toolbox config for live SQL Server queries
docs/
  design/             Architecture and design decision records
  reference/          Setup guides and reference docs
```

See `repo-map.json` for the full structure, entrypoints, and command reference.

---

## Development

### Worktrees

Use the single wrapper command to create or attach a worktree and bootstrap it:

```bash
./scripts/worktree.sh feature/<branch-name>
```

The wrapper creates the worktree under `../worktrees/<branch-name>`, symlinks `.env`, runs
`direnv allow` when available, installs Python dev dependencies in `lib`, verifies
`pyodbc` and `oracledb`, and installs eval dependencies in `tests/evals`.

If the requested branch is already checked out in another worktree, the script fails with
structured JSON on stderr that includes the existing worktree path so an agent can recover
deterministically.

### Tests

```bash
cd lib && uv run pytest                            # shared library
cd mcp/ddl && uv run pytest                        # DDL MCP server
cd lib && uv run pytest -m integration             # requires Docker SQL Server
```

### Lint

```bash
markdownlint <file>    # all .md files must pass before committing
```

### Design docs

Add a subdirectory under `docs/design/` with a `README.md`, then update `docs/design/README.md`.

---

## Contributing

- **Branch:** `feature/vu-<id>-short-description`
- **PR title:** `VU-XXX: short description`
- **PR body:** `Fixes VU-XXX`
- **Worktrees:** `../worktrees/<branchName>` — see `.claude/rules/git-workflow.md`
- **Commits:** one concern per commit; run tests before each

---

## License

[Elastic License 2.0](LICENSE) — free to use, not available as a managed service.
