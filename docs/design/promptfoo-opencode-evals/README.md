# Promptfoo OpenCode Evals

## Decision

The `tests/evals/` suite runs Promptfoo through OpenCode only. Package configs select named eval tiers, while suite-owned runtime and cleanliness guards resolve those selections into concrete Promptfoo provider configs at execution time.

## Why

The eval suite needs one place to change model and turn-budget policy without rewriting package YAMLs, and one place to enforce repo cleanup rules, artifact locations, and OpenCode lifecycle behavior.

## Boundaries

### Suite-owned configuration

- `tests/evals/config/eval-tiers.toml` is the source of truth for eval tiers and shared OpenCode runtime settings.
- Tier names are stable policy names: `light`, `standard`, `high`, `x_high`.
- Tier definitions own `model` and `max_turns`.
- Shared OpenCode runtime settings own provider id, base URL, working directory, and tool permissions.

### Package-owned configuration

- Each package YAML declares `metadata.eval_tier`.
- Packages own prompts, tests, assertions, and suite-specific test variables.
- Packages do not hardcode model ids, turn budgets, or provider-specific runtime settings.

## Runtime contract

### Resolution

- `tests/evals/scripts/promptfoo.sh` remains the suite entrypoint used by `tests/evals/package.json`.
- The suite runtime resolves each `-c <package-config>` input before Promptfoo executes it.
- Resolution loads the suite tier registry, validates `metadata.eval_tier`, and writes a resolved config under suite temp state.
- Promptfoo runs only against resolved suite-temp configs, never directly against unresolved package configs.

### OpenCode execution

- The suite runtime ensures an OpenCode server is reachable before Promptfoo starts.
- Resolved providers use `opencode:sdk`.
- The initial model policy is Qwen 3.6 for all four tiers; the tier registry still owns the mapping so future model changes stay suite-local.

### Cleanliness and artifacts

- Cleanliness guards are suite infrastructure, not package behavior.
- Eval runs may write only to suite-owned artifact roots under `tests/evals/`.
- Guard enforcement remains on the Promptfoo execution path regardless of selected package tier.

## Validation

- Suite tests validate that the tier registry defines all required tiers.
- Suite tests validate that every package declares a valid `metadata.eval_tier`.
- Suite tests validate that resolved configs contain `opencode:sdk`, Qwen 3.6, the tier-selected `max_turns`, and the required tool permissions.
- Existing cleanliness-guard tests remain the authority for repo-dirtiness enforcement.

## Extraction posture

This stays a suite-level framework inside `tests/evals/`. Keep the seams clean enough to extract later, but do not generalize beyond this suite until another project needs the same contract.
