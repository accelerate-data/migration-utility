# Stage 1 -- Project Init

`/init-ad-migration` is the entrypoint for a new migration repo. It checks prerequisites, scaffolds the project, and prepares the repo for the rest of the pipeline.

```text
/init-ad-migration
/init-ad-migration oracle
```

If you omit the source, the command prompts for it. Supported source families today are SQL Server and Oracle.

## What it checks

The command groups checks into common prerequisites plus source-specific checks.

### Common checks

- `uv`
- Python 3.11+
- shared Python dependencies
- git repository presence
- `direnv` availability

### SQL Server-specific checks

- FreeTDS and unixODBC registration
- optional `toolbox` for live `/setup-ddl`
- `MSSQL_*` environment variables

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
- `scripts/worktree.sh`
- `.githooks/pre-commit`

It also writes a partial `manifest.json` with source technology and dialect so `/setup-ddl` can enrich it later.

## Worktree flow

The scaffolded repo includes `scripts/worktree.sh` as the canonical way to create or attach a worktree and bootstrap it. That script is what downstream command docs and git-workflow guidance refer to.

## Idempotency

The command is safe to re-run:

- existing scaffold files are skipped or only filled in when sections are missing
- `.envrc` is not overwritten if you already edited it
- the scaffold commit only runs when files actually changed

## Next step

Proceed to [[Stage 2 DDL Extraction]].
