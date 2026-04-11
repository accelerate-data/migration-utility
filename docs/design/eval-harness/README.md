# Eval Harness

Non-interactive test harness for skills and commands. Uses [Promptfoo](https://github.com/promptfoo/promptfoo) with the `anthropic:claude-agent-sdk` provider to invoke Claude Code against pre-committed fixtures, then validates structured output.

---

## Architecture

```text
Offline regression (SQL Server + Oracle)
        │
        ▼  (fixtures pre-committed — no live DB needed)
Per-scenario fixtures (tests/evals/fixtures/<package>/<scenario-slug>/ or oracle-regression/fixtures/)
        │
        ▼  (Promptfoo invokes claude-agent-sdk per scenario)
Claude Code agent ──► reads fixture ──► calls Python CLIs ──► produces output
        │
        ▼  (Promptfoo validates output)
Assertions: custom JS validators check catalog JSON, dbt models, test specs, reviews

Live DB (optional — requires Docker)
        │
        ▼  (setup-ddl extract writes to fixture_path at runtime)
oracle-live / mssql-live packages — validate end-to-end extract → scope → profile
```

Skills are tested via Promptfoo scenarios that invoke them non-interactively and validate their output. Commands (multi-table batch orchestrators) are tested via separate command eval packages that exercise parallel dispatch, error handling, review loops, and summary aggregation.

---

## Prerequisites

- Node.js (for `npx promptfoo`)
- `ANTHROPIC_API_KEY` in your shell environment
- DDL project fixture extracted from MigrationTest (see [MAINTENANCE.md](MAINTENANCE.md))

---

## What To Run

All commands assume you are in `tests/evals/`.

```bash
cd tests/evals
npm install
```

If you want:

- A broad offline smoke pass across the default harness: `npm run eval`
- One skill package only: run the matching `npm run eval:<package>`
- One command package only: run the matching `npm run eval:<package>`
- Oracle offline regression against the SH fixture: `npm run eval:oracle-regression`
- Live end-to-end extract → scope → profile against Docker SQL Server: `npm run eval:mssql-live`
- Live end-to-end extract → scope → profile against Docker Oracle: `npm run eval:oracle-live`
- To inspect results in the browser: `npm run view`

### Common choices

Use these when you are working in a specific area:

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
| `/generate-model` command and model-generation fixtures | `npm run eval:cmd-generate-model` |
| `/generate-tests` command and test-generation fixtures | `npm run eval:cmd-generate-tests` |
| `/status` command output | `npm run eval:cmd-status` |
| `/commit-push-pr` failure handling | `npm run eval:cmd-commit-push-pr` |
| Oracle-specific offline behavior | `npm run eval:oracle-regression` |
| Live database extraction flow | `npm run eval:oracle-live` or `npm run eval:mssql-live` |

### Direct commands

```bash
# Broad offline pass
npm run eval

# Skill packages
npm run eval:listing-objects
npm run eval:analyzing-table
npm run eval:profiling-table
npm run eval:generating-tests
npm run eval:generating-model
npm run eval:reviewing-tests
npm run eval:refactoring-sql
npm run eval:reviewing-model


# Command packages
npm run eval:cmd-scope
npm run eval:cmd-profile
npm run eval:cmd-refactor
npm run eval:cmd-generate-model
npm run eval:cmd-generate-tests
npm run eval:cmd-status
npm run eval:cmd-commit-push-pr

# Dialect-specific packages
npm run eval:oracle-regression
npm run eval:oracle-live
npm run eval:mssql-live
```

`ANTHROPIC_API_KEY` must be in the environment. Promptfoo reads it automatically.

### Narrowing to one targeted eval

Start with the smallest package that owns the behavior you changed.

- If you changed skill behavior, run the owning skill package first.
- If you changed command orchestration, run the command package first.
- If you changed Oracle-specific handling, run `oracle-regression`.
- Only run `npm run eval` when you want a broader offline pass.
- Only run `oracle-live` or `mssql-live` when the issue is in live extraction or database-specific runtime behavior.

If you need one subset inside a package, run Promptfoo directly with `--filter-pattern`:

```bash
# One scenario family inside cmd-generate-tests
npx promptfoo eval -c packages/cmd-generate-tests/cmd-generate-tests.yaml --filter-pattern "merge"

# One review scenario family
npx promptfoo eval -c packages/reviewing-model/skill-reviewing-model.yaml --filter-pattern "review-standards"

# One generating-model baseline
npx promptfoo eval -c packages/generating-model/skill-generating-model.yaml --filter-pattern "snapshot"

# One refactor pattern
npx promptfoo eval -c packages/refactoring-sql/skill-refactoring-sql.yaml --filter-pattern "recursive-cte"
```

### Model generation coverage split

Use two layers for model generation work:

- `eval:generating-model` is the fast baseline for generator-owned invariants such as writing dbt artifacts, materialization shape, control columns, and snapshot rendering.
- `eval:cmd-generate-model` is the full-flow suite for readiness checks, multi-table orchestration, review loops, and final command summaries.

This keeps direct skill regressions easy to localize without making the command package carry every baseline artifact check.

### Fixture reset behavior

The package scripts restore their fixture roots before and after each run. Most offline package scripts reset `tests/evals/fixtures/`; the Oracle and live-DB scripts reset their package-local fixture roots; `eval:cmd-commit-push-pr` is text-only and does not restore fixtures because it does not operate on a fixture tree.

All eval scripts use `--no-cache` to force fresh LLM invocations.

If you add a new fixture directory, commit it or at least stage it before running the package script. The reset step uses `git clean`, so untracked fixture directories will be deleted.

### Mixed prompt packages

If a package contains both table and view scenarios, do not rely on the package-level `prompts:` list alone. Pin each scenario to its intended prompt with `prompts: ["<prompt-id>"]`.

Current examples include `packages/analyzing-table/skill-analyzing-table.yaml`, `packages/profiling-table/skill-profiling-table.yaml`, `packages/generating-tests/skill-generating-tests.yaml`, and `packages/reviewing-tests/skill-reviewing-tests.yaml`.

Use this whenever:

- the package has separate table and view prompts
- the package has object-type-specific prompts with different required vars
- adding a new scenario would otherwise expand against every prompt in the package

---

## When An Eval Fails

Check in this order:

1. The package YAML: prompt, fixture path, vars, and assertions.
2. The fixture directory: catalog JSON, DDL, dbt files, test specs.
3. The assertion module: what artifact it reads and what it expects.
4. The prompt file: whether the instruction is too weak, too broad, or outdated.

High-level map:

- Package YAMLs in `tests/evals/packages/` decide what runs.
- Top-level dialect packages in `tests/evals/oracle-regression/`, `tests/evals/oracle-live/`, and `tests/evals/mssql-live/` own Oracle/live flows.
- Fixtures in `tests/evals/fixtures/` or package-local `fixtures/` provide the test input state.
- Assertions in `tests/evals/assertions/` decide pass/fail from written artifacts.
- Prompts in `tests/evals/prompts/` shape agent behavior.

Typical failure questions:

- Wrong package? The run passed, but it did not exercise the behavior you changed.
- Wrong fixture? The scenario does not contain the state needed to reproduce the issue.
- Wrong assertion? The agent output is acceptable, but the assertion is outdated or too strict.
- Wrong prompt? The fixture and assertion are fine, but the prompt does not steer the agent well enough.

## Improving A Scenario

If you want to improve coverage or make an eval more reliable:

1. Find the owning package YAML.
2. Find the fixture directory used by that scenario.
3. Decide which layer is wrong:
   fixture, prompt, assertion, or package vars.
4. Rerun only that package or filtered scenario.

Use this rule of thumb:

- Change the fixture when the scenario does not represent the real failure mode.
- Change the prompt when the scenario is valid but the agent needs better instructions.
- Change the assertion when the expected outcome is right but the check is stale or too brittle.
- Change the package YAML when the wrong prompt, fixture, or vars are wired together.
- If a package mixes table and view scenarios, pin each scenario to the correct prompt before rerunning the package.

---

## More Detail

For harness internals and maintenance detail, see:

- [MAINTENANCE.md](MAINTENANCE.md)
- [SCENARIOS.md](SCENARIOS.md)
- [DIRECTORY.md](DIRECTORY.md)
