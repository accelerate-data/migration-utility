# Quickstart

Happy-path walkthrough for migrating two tables, `silver.DimCustomer` and `silver.FactInternetSales`.

## Prerequisites

- All tools installed and verified, see [[Installation and Prerequisites]]
- A git repo for the migration project
- Access to the source database

## 1. Scaffold the project and install the CLI

```text
/init-ad-migration
```

This installs the `ad-migration` CLI via Homebrew, checks prerequisites, writes the project starter files, and scaffolds `scripts/worktree.sh` plus the repo-local git-workflow guidance.

Generated files include:

- `CLAUDE.md`
- `README.md`
- `repo-map.json`
- `.envrc`
- `.githooks/pre-commit`
- `.claude/rules/git-workflow.md`
- `scripts/worktree.sh`

See [[Stage 1 Project Init]].

## 2. Extract DDL and build the catalog

```bash
ad-migration setup-source --technology sql_server --schemas silver,gold
```

This validates credentials, extracts DDL, and builds catalog files. It creates `manifest.json`, writes extracted DDL into `ddl/`, and builds per-object catalog files in `catalog/`.

For Oracle use `--technology oracle`.

See [[Stage 2 DDL Extraction]].

## 3. Resolve extracted tables before target setup

```text
/scope silver.DimCustomer silver.FactInternetSales
```

Before `/setup-target` can proceed, every extracted table needs one of these outcomes:

- scoped to a writer via `/scope` or `/analyzing-table`
- excluded from the migration via `ad-migration exclude-table <fqn>`
- confirmed as a source via `ad-migration add-source-table <fqn>`

Tables with `scoping.status == "no_writer_found"` are not automatically included in `sources.yml`; they stay pending until you explicitly confirm them as sources.

See [[Stage 1 Scoping]].

## 4. Set up the target

```bash
ad-migration setup-target --technology fabric
```

This scaffolds `dbt/`, persists `runtime.target`, and generates `models/staging/sources.yml`.

`sources.yml` includes only tables explicitly marked `is_source: true`. Writerless tables that have not been confirmed yet are shown as pending so you can decide whether to add them as sources.

See [[Stage 3 dbt Scaffolding]].

## 5. Profile the migration targets

```text
/profile silver.DimCustomer silver.FactInternetSales
```

This writes the migration profile for each object: classification, keys, watermark, and other downstream signals used by test generation and model generation.

See [[Stage 2 Profiling]].

## 6. Create the sandbox

```bash
ad-migration setup-sandbox
```

This creates the active sandbox endpoint used for ground-truth capture and SQL equivalence checks and persists it as `runtime.sandbox`.

See [[Stage 4 Sandbox Setup]].

## 7. Generate tests

```text
/generate-tests silver.DimCustomer silver.FactInternetSales
```

This generates scenarios, runs the independent review loop, executes approved scenarios in the sandbox, and writes dbt-ready YAML test artifacts.

See [[Stage 3 Test Generation]].

## 8. Refactor the source SQL

```text
/refactor silver.DimCustomer silver.FactInternetSales
```

This restructures the source SQL into import/logical/final CTE form and proves equivalence against the extracted ground truth. For table migrations, the proof-backed refactor is persisted on the selected writer procedure catalog entry.

See [[Stage 5 SQL Refactoring]].

## 9. Generate dbt models

```text
/generate-model silver.DimCustomer silver.FactInternetSales
```

This generates dbt SQL and schema YAML, runs `dbt build`, applies the independent model review loop, and commits successful items.

See [[Stage 4 Model Generation]].

## 10. Check overall progress

```text
/status
```

Use this to see what is complete, what is blocked, which source tables still need confirmation, and what command to run next.

See [[Status Dashboard]].

## 11. Tear down the sandbox when finished

```bash
ad-migration teardown-sandbox
```

Run this after all test generation and refactor work that depends on the sandbox is complete.

## 12. Clean up merged worktrees later

```bash
bash scripts/cleanup-worktrees.sh
```

After PRs are merged, use this to remove stale worktrees and merged branches.
