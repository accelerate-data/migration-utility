# Contributing

Use `README.md` for setup and `AGENTS.md` for repository conventions.

## Workflow

1. Create or reference a Linear issue.
2. Work from a feature branch or worktree.
3. Keep commits focused on one concern.
4. Run the relevant tests and linters before opening a PR.
5. Open a PR titled `VU-XXX: short description` with `Fixes VU-XXX` in the body.

## Local Checks

Run focused checks for the files you changed:

```bash
markdownlint <changed.md>
cd lib && uv run pytest
cd mcp/ddl && uv run pytest
```

For Python changes, also run Ruff:

```bash
uvx ruff check lib/shared packages/ad-migration-cli/src packages/ad-migration-internal/src mcp/ddl scripts tests --select F401,F841
```

Integration tests require local database infrastructure. Document any skipped infrastructure-dependent checks in the PR.
