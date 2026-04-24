# Promptfoo OpenCode Evals

## Decision

The `tests/evals/` suite runs Promptfoo through OpenCode only. Package configs select named eval tiers, while suite-owned runtime and cleanliness guards resolve those selections into concrete Promptfoo provider configs at execution time.

## Why

The move to OpenCode with Qwen 3.6 is primarily a cost decision. Because the cutover requires touching every eval package and rerunning the suite, the same change also reorganizes the suite boundary: centralize runtime policy, re-evaluate package layout, and remove evals that do not justify their maintenance cost.

## Boundaries

### Suite-owned configuration

- `tests/evals/config/eval-tiers.toml` is the source of truth for eval tiers and shared OpenCode runtime settings.
- Tier names are stable policy names: `light`, `standard`, `high`, `x_high`.
- Tier definitions own `max_turns`.
- Shared OpenCode runtime settings own provider id, model provider id, model, working directory, empty-output retry policy, and tool permissions.

### Package-owned configuration

- Each package YAML declares `metadata.eval_tier`.
- Packages own prompts, tests, assertions, and suite-specific test variables.
- Packages do not hardcode model ids, turn budgets, or provider-specific runtime settings.

## Suite maintenance posture

- The OpenCode cutover is also the suite reorganization point because all packages must be reviewed and rerun anyway.
- During the migration, packages may be renamed, merged, or regrouped when that improves suite clarity.
- Evals that no longer provide meaningful coverage, catch real regressions, or justify their runtime cost should be removed instead of carried forward unchanged.
- The suite keeps only packages with a clear contract and ongoing signal value.

## Runtime contract

### Resolution

- `tests/evals/scripts/promptfoo.sh` remains the suite entrypoint used by `tests/evals/package.json`.
- The suite runtime resolves each `-c <package-config>` input before Promptfoo executes it.
- Resolution loads the suite tier registry, validates `metadata.eval_tier`, and writes a resolved config under suite temp state.
- Promptfoo runs only against resolved suite-temp configs, never directly against unresolved package configs.

### OpenCode execution

- Resolved providers use the suite local provider `file://scripts/opencode-cli-provider.js`.
- The provider invokes `opencode run --model opencode/qwen3.6-plus --agent build` for each Promptfoo test case.
- The suite does not start or manage `opencode serve`.
- Evals use OpenCode CLI execution only.
- The initial model policy is Qwen 3.6 for all four tiers; the tier registry still owns the mapping so future model changes stay suite-local.

### Cleanliness and artifacts

- Cleanliness guards are suite infrastructure, not package behavior.
- Eval runs may write only to suite-owned artifact roots under `tests/evals/`.
- Guard enforcement remains on the Promptfoo execution path regardless of selected package tier.

## Validation

- Suite tests validate that the tier registry defines all required tiers.
- Suite tests validate that every package declares a valid `metadata.eval_tier`.
- Suite tests validate that resolved configs contain the local OpenCode provider, Qwen 3.6, the tier-selected `max_turns`, and the required tool permissions.
- Provider tests validate OpenCode/Qwen invocation, empty-output retry behavior, and retry-count validation.
- Existing cleanliness-guard tests remain the authority for repo-dirtiness enforcement.

## Extraction posture

This stays a suite-level framework inside `tests/evals/`. Keep the seams clean enough to extract later, but do not generalize beyond this suite until another project needs the same contract.
