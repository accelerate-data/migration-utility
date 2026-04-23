# Migration Utility

A Claude Code plugin that migrates warehouse stored procedures to dbt models. Targets silver and gold transformations from SQL-based sources; non-SQL runtimes are out of scope.

See `AGENTS.md` for architecture, conventions, and agent guidance.

---

## Contributor Setup

This README is the canonical onboarding path for developing `migration-utility` itself.

It is written for a human contributor on a fresh laptop who may be working through a coding agent.

This setup is for maintaining this repository. It is not the customer or migration-project setup flow, and it does not replace `/init-ad-migration`.

### Supported platforms

- `macOS`
- `Linux/Unix-like`

Windows is not supported by the contributor bootstrap flow.

### Maintainer-ready target

`maintainer ready` means:

- the repo can be edited, linted, and unit-tested
- repo-local environments for `lib/`, `mcp/ddl/`, and `tests/evals/` are bootstrapped
- Docker is installed and the daemon is reachable
- contributor containers can be started
- at least one live maintainer backend path works end to end: SQL Server or Oracle

### Canonical commands

Use the contributor bootstrap script from the repo root:

```bash
./scripts/contributor-setup.sh
```

This is the default `fix` mode. It runs repo-local bootstrap, verifies maintainer readiness, and ends with a human-readable summary plus trailing JSON for coding agents.

For a non-mutating report:

```bash
./scripts/contributor-setup.sh show
```

### What the script checks

- supported OS
- required vs optional machine-level tools
- repo-local bootstrap inputs
- `lib/` and `mcp/ddl/` Python environments
- `tests/evals/` dependencies
- Docker binary and daemon readiness
- SQL Server contributor path
- Oracle contributor path

### What the script can do automatically

- sync the repo-local Python environments in `lib/` and `mcp/ddl/`
- install repo-local eval harness dependencies in `tests/evals/`
- verify Docker and contributor container readiness

### What still requires user action

- installing machine-level tools
- Docker login/image pull/setup when containers are missing
- fixing host-level toolchain or permission issues

### Status meanings

- `ready`: Docker works, repo-local bootstrap succeeded, and at least one backend path works
- `partially_ready`: the repo is close but still needs manual follow-up
- `blocked`: the environment cannot satisfy maintainer readiness yet

---

## Machine Tools

### Required tools

| Tool | Purpose |
|------|---------|
| `git` | Version control and worktree support |
| Python 3.11+ | Runtime for `lib/` and `mcp/` |
| [uv](https://docs.astral.sh/uv/) | Python package manager |
| Node.js + npm | Promptfoo eval harness for migration-only evals (`tests/evals/`) |
| [direnv](https://direnv.net/) | Auto-loads `.env` credentials |
| Docker | Contributor container and integration readiness |
| [markdownlint-cli](https://github.com/igorshubovych/markdownlint-cli) | All `.md` files must pass before commit |
| [`toolbox`](https://github.com/googleapis/genai-toolbox/releases) | SQL Server maintainer path |

### Optional tools

| Tool | Purpose |
|------|---------|
| [gh CLI](https://cli.github.com/) | GitHub API interactions |

### Environment variables

Fill in `.env` (commented examples are included), then:

```bash
direnv allow
```

See [docs/wiki/Installation-and-Prerequisites.md](docs/wiki/Installation-and-Prerequisites.md) for the detailed environment-variable reference.

---

## Fresh-Laptop Flow

1. Clone the repo.
2. Run `./scripts/contributor-setup.sh`.
3. Follow any manual actions it reports.
4. Re-run `./scripts/contributor-setup.sh` until it reports `ready`.
5. Use `./scripts/contributor-setup.sh show` later when you want a non-mutating status check.

```bash
git clone https://github.com/accelerate-data/migration-utility
cd migration-utility
./scripts/contributor-setup.sh
```

### Manual Docker setup reference

The contributor bootstrap script verifies Docker and contributor containers, but it does not pull images or log in to registries for you.

Use [docs/reference/setup-docker/README.md](docs/reference/setup-docker/README.md) for the one-time Docker image and container setup.

### Local plugin execution

To run the plugin locally:

```bash
claude --plugin-dir .
```

This assumes the relevant MCP prerequisites are already installed and on `PATH`.

Codex marketplace installation reads `.codex-plugin/plugin.json`. Codex supports the root `skills/` surface and the root `.mcp.json` DDL MCP server; root `commands/` remain Claude-only. See [docs/reference/codex-plugin-surface/README.md](docs/reference/codex-plugin-surface/README.md).

---

## Repository Structure

```text
.claude/              Agent rules, skills, and memory
.codex-plugin/        Codex plugin manifest
agents/               Claude agent definitions
commands/             Claude-only slash command specs
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

- **Contributor bootstrap:** `./scripts/contributor-setup.sh`
- **Branch:** `feature/vu-<id>-short-description`
- **PR title:** `VU-XXX: short description`
- **PR body:** `Fixes VU-XXX`
- **Worktrees:** `../worktrees/<branchName>` — see `.claude/rules/git-workflow.md`
- **Commits:** one concern per commit; run tests before each

---

## License

[Elastic License 2.0](LICENSE) — free to use, not available as a managed service.
