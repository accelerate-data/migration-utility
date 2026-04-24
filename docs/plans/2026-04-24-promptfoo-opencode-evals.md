# Promptfoo OpenCode Evals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task.

## Goal

Move `tests/evals/` to Promptfoo execution through OpenCode on Qwen 3.6, centralize suite runtime policy, and use the forced full-suite rerun to simplify package layout.

## Runtime Decision

Package YAMLs remain the suite-facing eval surface, but they no longer own provider config. Each package declares `metadata.eval_tier`. The suite resolver reads `tests/evals/config/eval-tiers.toml`, materializes a resolved Promptfoo config under `tests/evals/.tmp/resolved-configs/`, and injects the suite-owned local provider `file://scripts/opencode-cli-provider.js`.

The provider runs `opencode run --model opencode/qwen3.6-plus --agent build` per Promptfoo test case. The suite does not start or manage `opencode serve`, and there is no fallback provider.

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

## Pending Work

- [x] Run the full deterministic eval test suite: `cd tests/evals && npm test`.
- [x] Run full `cmd-status`: `cd tests/evals && npm run eval:cmd-status`.
- [x] Run suite smoke evals: `cd tests/evals && npm run eval:smoke`.
- [x] Run Markdown and whitespace checks for changed docs/code.
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
