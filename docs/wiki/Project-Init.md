# Project Init

`/init-ad-migration` is the entrypoint for a new migration repo. It installs the `ad-migration` CLI via Homebrew on macOS, reports the supported Linux/WSL install path when needed, checks prerequisites, scaffolds the project, and prepares the repo for the pipeline. Native Windows is not supported. Use WSL for the local workflow.

```text
/init-ad-migration
/init-ad-migration oracle
```

If you omit the source, the command prompts for it. Supported source families today are SQL Server and Oracle.

## What it checks

The command groups checks into common prerequisites plus source-specific checks.

### Common checks

- Python 3.11+
- `ad-migration` CLI installed (Homebrew on macOS; GitHub release wheel artifacts on Linux and WSL)
- git repository presence
- `direnv` availability

### SQL Server-specific checks

- FreeTDS and unixODBC registration
- source connection variables for `ad-migration setup-source`
- optional `MSSQL_DRIVER` override when you are not using the default `FreeTDS` path

### Oracle-specific checks

- optional SQLcl
- Java 11+
- Oracle connection environment variables

Optional checks do not block scaffolding; they tell you what still needs to be installed before live extraction.

## What it scaffolds

The project scaffold currently includes:

- `CLAUDE.md`
- `README.md`
- `repo-map.json`
- `.gitignore`
- `.envrc`
- `.claude/rules/git-workflow.md`
- `.githooks/pre-commit`

It also writes a partial `manifest.json` with source technology and dialect so `ad-migration setup-source` can enrich it later.

## Worktree flow

The scaffolded repo does not include a repo-local worktree wrapper script. Batch commands check the current branch and can create a feature-branch worktree when you choose that option from the default-branch prompt. The scaffolded git-workflow guidance documents the resulting worktree location and cleanup behavior.

## Idempotency

The command is safe to re-run:

- existing scaffold files are skipped or only filled in when sections are missing
- `.envrc` is not overwritten if you already edited it
- the scaffold commit only runs when files actually changed

## Next step

Proceed to [[DDL Extraction]] and run `ad-migration setup-source`.
