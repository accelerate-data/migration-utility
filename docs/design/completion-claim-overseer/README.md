# Completion Claim Overseer

## Decision

Migration agents use a completion-claim overseer before stating that work is complete, successful, passing, PR-ready, merged, or coordinator-stage-complete.

The overseer is triggered by claim intent. Slash commands and migration skills do not call it as an explicit workflow step.

## Reason

Migration workflows combine LLM judgment, generated artifacts, deterministic CLI output, catalog writeback, git state, and PR handoff.

An agent can finish a nominal flow and still report an unsupported claim when evidence is stale, partial, missing, or only inferred from a sub-agent report.

Completion wording is therefore a verified output, not a default ending.

## Runtime Guidance

The completion-claim rule is seeded into migration project `CLAUDE.md`.

This repository's `AGENTS.md` remains maintainer-facing and does not carry runtime migration-project policy. Generated project `CLAUDE.md` is the right authority because slash commands, skills, and sub-agents operate from that project context.

`init-ad-migration` owns the migration-project `CLAUDE.md` contract. New projects receive the completion-claim section during scaffold creation.

For existing projects, scaffold refreshes must preserve local content and append missing managed sections. The command must not stop at telling the user to add required sections manually. Interactive flows may ask for confirmation before editing, but applying approved missing-section updates remains the slash command's responsibility.

## Overseer Boundary

The overseer verifies claims, not workflows.

It does not replace stage guards, readiness checks, review skills, dbt validation, SQL comparison, or PR scripts. It checks whether the agent has enough fresh evidence to say what it is about to say.

Sub-agent reports are not sufficient evidence by themselves. The parent agent must inspect the produced artifact, diff, command output, PR state, or coordinator plan state before repeating a success claim.

## Evidence Model

Before a completion claim, the overseer asks the agent to identify:

- the exact claim about to be made;
- the command, artifact, or state that proves the claim;
- whether the evidence is fresh from the current run;
- whether the agent read the output, exit code, artifact, or state directly; and
- whether the evidence supports the exact wording.

Common evidence includes run artifacts, summary files, catalog writeback, validated review JSON, dbt output, SQL comparison output, git state, PR state, and coordinator plan state.

## Outcomes

The overseer allows three communication outcomes:

- verified claim: evidence supports the exact wording;
- downgraded claim: evidence supports only partial, warning, blocked, skipped, or unknown state;
- blocked claim: evidence is missing or contradicts the intended wording.

Agents must not use completion language when the overseer blocks or downgrades the claim.

## Seeded Section

Seeded migration project `CLAUDE.md` should include a managed section equivalent to:

```md
## Completion Claims

Before stating that work is complete, successful, passing, PR-ready, merged, or stage-complete, run the completion-claim verification skill.

Verify fresh evidence for the exact claim: command output, exit code, run artifact, catalog writeback, dbt result, comparison result, git state, PR state, or coordinator plan state.

Do not repeat a sub-agent's success claim without inspecting the evidence it produced. If evidence is partial, stale, missing, or contradictory, report the actual state instead of using completion language.
```
