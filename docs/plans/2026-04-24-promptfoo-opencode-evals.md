# Promptfoo OpenCode Evals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task.

## Goal

Move `tests/evals/` to Promptfoo execution through OpenCode on Qwen 3.6, centralize suite runtime policy, and use the forced full-suite rerun to simplify package layout.

## Runtime Decision

Package YAMLs remain the suite-facing eval surface, but they no longer own provider config. Each package declares `metadata.eval_tier`. The suite resolver reads `tests/evals/config/eval-tiers.toml`, materializes a resolved Promptfoo config under `tests/evals/.tmp/resolved-configs/`, and injects the suite-owned local provider `file://scripts/opencode-cli-provider.js`.

The final harness design is: TOML maps each tier to an OpenCode agent, and `tests/evals/opencode.json` defines those agents. Agents own `model`, `steps`, `permission`, and optional tuning. The provider runs `opencode run` with the tier-selected agent, explicit project directory, output format, log level, and suite OpenCode config path through `OPENCODE_CONFIG`.

The suite does not start or manage `opencode serve`, and there is no fallback provider.

## Current Status

- [x] Suite tier registry exists in `tests/evals/config/eval-tiers.toml` with `light`, `standard`, `high`, and `x_high`.
- [x] Runtime config selects OpenCode Qwen via `model_provider_id = "opencode"` and `model = "qwen3.6-plus"`.
- [x] Runtime config owns OpenCode retry policy for empty CLI stdout via `empty_output_retries`.
- [x] Resolver materializes package configs with the suite provider, tier-selected `max_turns`, shared tool permissions, and stable absolute provider paths.
- [x] Promptfoo wrapper and guard resolve configs before execution and enforce suite-level cleanliness.
- [x] Wrapper no longer manages OpenCode server lifecycle.
- [x] Package and live configs use `metadata.eval_tier`; old provider YAMLs are removed.
- [x] `analyzing-table-readiness` was folded into `analyzing-table`.
- [x] Deterministic harness tests pass after the CLI provider cutover.
- [x] The previously failing `cmd-status` `status-all-summary` case passes through real OpenCode/Qwen with the empty-output guard.
- [x] Design doc updated so the intended final state has TOML tier-to-agent mapping and agent-owned model/steps/permissions in `opencode.json`.

## Pending Work

- [ ] Refactor `eval-tiers.toml` so runtime owns only enforced harness inputs: provider path, `opencode_config`, `project_dir`, format, log level, print-log flag, and empty-output retry policy.
- [ ] Refactor tiers so each tier maps to an agent name instead of `max_turns`.
- [ ] Refactor `opencode.json` to define primary eval agents (`eval_light`, `eval_standard`, `eval_high`, `eval_x_high`) with `model`, `steps`, `permission`, and optional model tuning.
- [ ] Refactor config loading and validation so every TOML tier points to an existing OpenCode agent and every eval agent defines enforceable fields.
- [ ] Refactor resolved Promptfoo configs so provider config contains the tier-selected agent and no unenforced `max_turns`, tool, model-provider split, or duplicated model fields.
- [ ] Refactor the OpenCode CLI provider to set `OPENCODE_CONFIG`, pass `--agent`, `--dir`, `--format`, `--log-level`, optional `--print-logs`, and stop passing `--model`.
- [ ] Update tests for the new TOML/OpenCode-agent contract and verify current tests fail before the implementation patch.
- [ ] Run focused deterministic tests for the resolver, tier config, provider, suite contract, and guard.
- [ ] Run `cd tests/evals && npm test`.
- [ ] Run at least one real OpenCode eval package after the redesign, starting with `cd tests/evals && npm run eval:cmd-status -- --filter-pattern 'status-all-summary'`.
- [ ] Run full `cmd-status`: `cd tests/evals && npm run eval:cmd-status`.
- [ ] Run suite smoke evals: `cd tests/evals && npm run eval:smoke`.
- [ ] Run Markdown and whitespace checks for changed docs/code.
- [ ] Commit, push, and update VU-1132 / PR with verification evidence.

## Concurrency Decision

Promptfoo evals default to one test at a time. Earlier shared OpenCode server attempts produced empty responses and session failures at higher concurrency; the current CLI provider isolates each test case, but serial execution remains the default cost/stability baseline. Revisit parallel execution only after the full suite is stable on Qwen.

## Files

| Action | File |
|---|---|
| Create | `tests/evals/config/eval-tiers.toml` |
| Create | `tests/evals/scripts/eval-tier-config.js` |
| Create | `tests/evals/scripts/eval-tier-config.test.js` |
| Create | `tests/evals/scripts/resolve-promptfoo-config.js` |
| Create | `tests/evals/scripts/resolve-promptfoo-config.test.js` |
| Create | `tests/evals/scripts/opencode-cli-provider.js` |
| Create | `tests/evals/scripts/opencode-cli-provider.test.js` |
| Modify | `tests/evals/opencode.json` |
| Modify | `tests/evals/scripts/promptfoo.sh` |
| Modify | `tests/evals/scripts/run-promptfoo-with-guard.js` |
| Modify | `tests/evals/scripts/run-promptfoo-with-guard.test.js` |
| Modify | `tests/evals/scripts/run-workspace-extension.test.js` |
| Modify | `tests/evals/package.json` |
| Modify | `tests/evals/package-lock.json` |
| Delete | `tests/evals/providers/*.yaml` |
| Delete | `tests/evals/providers/provider-tools.test.js` |
| Modify | package YAMLs under `tests/evals/packages/` plus live suite YAMLs |
| Modify | `docs/design/promptfoo-opencode-evals/README.md` |
| Modify | `repo-map.json` |
