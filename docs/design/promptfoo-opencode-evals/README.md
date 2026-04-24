# Promptfoo OpenCode Evals

## Decision

The `tests/evals/` suite runs Promptfoo through `opencode run` only. Package configs select named eval tiers, suite TOML maps those tiers to OpenCode agents, and suite OpenCode config defines the agents that enforce model, turn, and permission policy.

## Why

The move to OpenCode with Qwen 3.6 is primarily a cost decision. Because the cutover requires touching every eval package and rerunning the suite, the same change also reorganizes the suite boundary: centralize runtime policy, re-evaluate package layout, and remove evals that do not justify their maintenance cost.

## Boundaries

### Suite-owned configuration

- `tests/evals/config/eval-tiers.toml` is the source of truth for eval tiers and shared OpenCode runtime settings that the harness consumes directly.
- `tests/evals/opencode.json` is the source of truth for OpenCode-enforced agent behavior.
- Tier names are stable policy names: `light`, `standard`, `high`, `x_high`.
- Tier definitions select OpenCode agent names; they do not duplicate the agent's enforced settings.
- Shared runtime settings own the Promptfoo provider path, OpenCode config path, project directory, output format, log settings, and empty-output retry policy.
- OpenCode agents own `model`, `steps`, `permission`, and optional model tuning because those are the settings OpenCode enforces.

### Package-owned configuration

- Each package YAML declares `metadata.eval_tier`.
- Packages own prompts, tests, assertions, and suite-specific test variables.
- Packages do not hardcode model ids, OpenCode agents, turn budgets, permissions, or provider-specific runtime settings.

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
- The provider invokes `opencode run` for each Promptfoo test case with arguments rendered from `eval-tiers.toml`.
- The provider sets `OPENCODE_CONFIG` to the resolved suite OpenCode config path instead of relying on process-directory config discovery.
- The provider passes `--dir` so the OpenCode project workspace is explicit.
- The suite does not start or manage `opencode serve`.
- Evals use OpenCode CLI execution only.
- The initial model policy is Qwen 3.6 for all agents; model changes remain suite-local in `opencode.json`.
- Promptfoo evals default to `--max-concurrency 4`; callers can override concurrency explicitly.

The command template is:

```sh
OPENCODE_CONFIG=<runtime.opencode_config> \
opencode run \
  --agent <tiers[metadata.eval_tier].agent> \
  --dir <runtime.project_dir> \
  --format <runtime.format> \
  --log-level <runtime.log_level> \
  "<rendered prompt>"
```

`--print-logs` is included only when `runtime.print_logs` is true.

The suite TOML shape is:

```toml
[runtime]
provider_id = "file://scripts/opencode-cli-provider.js"
opencode_config = "opencode.json"
project_dir = "../.."
format = "default"
log_level = "ERROR"
print_logs = false
empty_output_retries = 1

[tiers.light]
agent = "eval_light"

[tiers.standard]
agent = "eval_standard"

[tiers.high]
agent = "eval_high"

[tiers.x_high]
agent = "eval_x_high"
```

The suite OpenCode config defines the selected agents:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "opencode": {
      "options": {
        "timeout": false
      }
    }
  },
  "agent": {
    "eval_light": {
      "description": "Promptfoo eval agent for light-cost scenarios.",
      "mode": "primary",
      "model": "opencode/qwen3.6-plus",
      "temperature": 0.1,
      "steps": 60,
      "permission": {
        "read": "allow",
        "write": "allow",
        "edit": "allow",
        "bash": "allow",
        "grep": "allow",
        "glob": "allow",
        "list": "allow",
        "webfetch": "deny"
      }
    }
  }
}
```

Each tier agent must be a primary agent so `opencode run --agent <name>` can execute it directly.
Eval agents are kept inline in `tests/evals/opencode.json` while they remain short policy definitions.
If an eval agent needs a substantial custom prompt, move that agent to `.opencode/agents/<agent>.md` and keep the TOML tier mapping unchanged.

### Cleanliness and artifacts

- Cleanliness guards are suite infrastructure, not package behavior.
- Eval runs may write only to suite-owned artifact roots under `tests/evals/`.
- Guard enforcement remains on the Promptfoo execution path regardless of selected package tier.

## Validation

- Suite tests validate that the tier registry defines all required tiers.
- Suite tests validate that every package declares a valid `metadata.eval_tier`.
- Suite tests validate that each TOML tier points to an agent defined in `opencode.json`.
- Suite tests validate that tier agents define `model`, `steps`, and `permission`.
- Suite tests validate that resolved configs contain the local OpenCode provider and tier-selected agent.
- Provider tests validate OpenCode invocation arguments, `OPENCODE_CONFIG`, `--dir`, logging flags, empty-output retry behavior, and retry-count validation.
- Existing cleanliness-guard tests remain the authority for repo-dirtiness enforcement.

## Extraction posture

This stays a suite-level framework inside `tests/evals/`. Keep the seams clean enough to extract later, but do not generalize beyond this suite until another project needs the same contract.
