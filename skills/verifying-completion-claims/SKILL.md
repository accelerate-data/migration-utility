---
name: verifying-completion-claims
description: >
  Use automatically before any completion, successful, passing, PR-ready, merged, or stage-complete statement to verify fresh evidence and choose accurate claim wording.
user-invocable: false
argument-hint: "<intended-claim-and-context>"
---

# Verifying Completion Claims

Verify completion-claim wording before the agent says work is complete, successful, passing, PR-ready, merged, or stage-complete.

This skill is self-contained. Use it because the next response would make a completion claim, even when no slash command or seeded `CLAUDE.md` guidance explicitly called it.

## Inputs

The caller supplies free-form context:

- the exact claim the agent is about to make;
- work just performed;
- known run IDs, object lists, plan files, branches, PRs, or validation commands; and
- evidence already inspected.

Ask for no extra context when the local workspace can answer the evidence question directly.

## Evidence Loop

1. Identify the exact claim the agent intends to make.
2. Determine the minimum fresh evidence that would prove that claim.
3. Inspect that evidence directly.
4. Compare the evidence to the intended wording.
5. Return verified, downgraded, or blocked wording.

Do not infer success from confidence, elapsed workflow steps, expected side effects, or sub-agent summaries.

## Evidence Sources

Use the narrowest fresh evidence that proves the claim:

- command output and exit code;
- `.migration-runs/` item and summary artifacts;
- catalog writeback state;
- validated review JSON;
- dbt compile, parse, or test output;
- SQL comparison output or explicit skip reason;
- git diff, commit, push, and branch state;
- PR state; and
- coordinator plan state.

Sub-agent reports are not sufficient evidence. Before repeating a sub-agent success claim, inspect the artifact, command output, diff, PR state, or plan state that proves it.

## Output

Return concise claim guidance:

- `verified`: the exact claim is supported and may be stated.
- `downgraded`: only a narrower claim is supported; provide replacement wording.
- `blocked`: no completion claim should be made; name the missing or contradictory evidence.

Include evidence bullets only when they help the final response stay precise.

## Common Mistakes

- Saying "complete" because all planned steps were attempted.
- Saying tests pass from an earlier run or partial command.
- Saying a PR is ready without checking git and PR state.
- Repeating a sub-agent success report without inspecting its output.
- Upgrading `partial`, `blocked`, skipped, or warning evidence into success wording.
