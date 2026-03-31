# Migration Utility

A Claude Code plugin that migrates Microsoft Fabric Warehouse stored procedures to dbt models. Targets silver and gold transformations from T-SQL; Lakehouse/Spark is out of scope.

See `AGENTS.md` for architecture, conventions, and agent guidance.

---

## Prerequisites

### Required tools

| Tool | Purpose |
|------|---------|
| Python 3.11+ | Runtime for `lib/` and `mcp/` |
| [uv](https://docs.astral.sh/uv/) | Python package manager |
| Node.js + npm | Promptfoo eval harness (`tests/evals/`) |
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

Fill in `.env` (commented examples are included) — key variables include `SA_PASSWORD` and `ANTHROPIC_API_KEY`. Then:

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
claude --plugin-dir plugins/
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

### Tests

```bash
cd lib && uv run pytest                     # shared library
cd mcp/ddl && uv run pytest                 # DDL MCP server
cd lib && uv run pytest -m integration      # requires Docker SQL Server
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
