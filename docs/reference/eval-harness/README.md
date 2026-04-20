# Eval Harness

Operational guide for the non-interactive harness under `tests/evals/`.

The harness uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against pre-committed fixtures and validate the resulting artifacts.

## Architecture

```text
Offline regression (SQL Server + Oracle)
        │
        ▼  (fixtures pre-committed — no live DB needed)
Per-scenario fixtures (tests/evals/fixtures/<package>/<scenario-slug>/ or package-local fixtures/)
        │
        ▼  (Promptfoo invokes claude-agent-sdk per scenario)
Claude Code agent ──► reads fixture ──► calls Python CLIs ──► produces output
        │
        ▼  (Promptfoo validates output)
Assertions: JS validators check catalog JSON, dbt models, test specs, reviews

Live DB (optional — requires Docker)
        │
        ▼  (setup-ddl extract writes to fixture_path at runtime)
oracle-live / mssql-live packages — validate end-to-end extract → scope → profile
```

Skills are tested via Promptfoo scenarios that invoke them non-interactively and validate their output. Commands are tested via separate eval packages that exercise orchestration, error handling, review loops, and summary aggregation.

## Prerequisites

- Node.js
- `ANTHROPIC_API_KEY` in the shell environment
- relevant fixtures or live Docker databases, depending on the package you run

## What To Run

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals
npm install
```

Common runs:

- curated offline smoke pass across all package configs: `npm run eval:smoke`
- live SQL Server extract → scope → profile: `npm run eval:mssql-live`
- live Oracle extract → scope → profile: `npm run eval:oracle-live`
- inspect results in the browser: `npm run view`
- Oracle post-extract command coverage lives in the owning command package configs, so there is no standalone `oracle-regression` run.

### Common choices

| If you changed | Run |
|---|---|
| Catalog browsing / `listing-objects` behavior | `npm run eval:listing-objects` |
| Profiling behavior | `npm run eval:profiling-table` |
| Table scoping / writer selection | `npm run eval:analyzing-table` or `npm run eval:cmd-scope` |
| Test generation skill behavior | `npm run eval:generating-tests` |
| Test review behavior | `npm run eval:reviewing-tests` |
| Model generation skill baseline behavior | `npm run eval:generating-model` |
| Model review behavior | `npm run eval:reviewing-model` |
| SQL refactoring behavior | `npm run eval:refactoring-sql` or `npm run eval:cmd-refactor` |
| `/profile` command orchestration | `npm run eval:cmd-profile` |
| `/generate-model` command orchestration | `npm run eval:cmd-generate-model` |
| `/generate-tests` command orchestration | `npm run eval:cmd-generate-tests` |
| `/status` command output | `npm run eval:cmd-status` |
| Live database extraction flow | `npm run eval:oracle-live` or `npm run eval:mssql-live` |

### Narrowing to one targeted eval

Start with the smallest package that owns the behavior you changed.

- skill behavior: run the owning skill package first
- command orchestration: run the owning command package first
- Oracle post-extract command handling: run the smallest owning command package config that covers the behavior; Oracle coverage now lives in specific command package YAMLs, not a standalone `oracle-regression` suite
- live extraction/runtime behavior: run `oracle-live` or `mssql-live`

If you need one subset inside a package, run Promptfoo directly with `--filter-pattern`:

```bash
npx promptfoo eval -c packages/cmd-generate-tests/cmd-generate-tests.yaml --filter-pattern "merge"
npx promptfoo eval -c packages/reviewing-model/skill-reviewing-model.yaml --filter-pattern "review-standards"
npx promptfoo eval -c packages/generating-model/skill-generating-model.yaml --filter-pattern "snapshot"
npx promptfoo eval -c packages/refactoring-sql/skill-refactoring-sql.yaml --filter-pattern "recursive-cte"
```

### Model generation coverage split

Use two layers for model-generation changes:

- `eval:generating-model` for generator-owned invariants such as artifact writing, materialization shape, control columns, and snapshot rendering
- `eval:cmd-generate-model` for readiness checks, orchestration, review loops, and final command summaries

### Fixture reset behavior

The package scripts restore their fixture roots before and after each run.

- most offline package scripts reset `tests/evals/fixtures/`
- Oracle and live-DB scripts reset their package-local fixture roots
All eval scripts use `--no-cache` to force fresh LLM invocations.

If you add a new fixture directory, commit or stage it before running the package script. The reset step uses `git clean`, so untracked fixture directories will be deleted.

### Mixed prompt packages

If a package contains both table and view scenarios, do not rely on the package-level `prompts:` list alone. Pin each scenario to its intended prompt with `prompts: ["<prompt-id>"]`.

Use this whenever:

- the package has separate table and view prompts
- the package has object-type-specific prompts with different required vars
- adding a new scenario would otherwise expand against every prompt in the package

## When An Eval Fails

Check in this order:

1. The package YAML: prompt, fixture path, vars, and assertions.
2. The fixture directory: catalog JSON, DDL, dbt files, test specs.
3. The assertion module: what artifact it reads and what it expects.
4. The prompt file: whether the instruction is too weak, too broad, or outdated.

High-level map:

- `tests/evals/packages/` holds the main offline package YAMLs
- `tests/evals/fixtures/oracle-regression/` holds Oracle regression fixtures consumed by the owning package configs
- `tests/evals/oracle-live/` and `tests/evals/mssql-live/` hold the standalone live suites
- `tests/evals/fixtures/` and package-local `fixtures/` hold input state
- `tests/evals/assertions/` decides pass/fail from written artifacts
- `tests/evals/prompts/` shapes agent behavior

Typical failure questions:

- wrong package?
- wrong fixture?
- wrong assertion?
- wrong prompt?

## Improving A Scenario

1. Find the owning package YAML.
2. Find the fixture directory used by that scenario.
3. Decide which layer is wrong: fixture, prompt, assertion, or package vars.
4. Rerun only that package or filtered scenario.

Rule of thumb:

- change the fixture when the scenario does not represent the real failure mode
- change the prompt when the scenario is valid but the agent needs better instructions
- change the assertion when the expected outcome is right but the check is stale or too brittle
- change the package YAML when the wrong prompt, fixture, or vars are wired together
