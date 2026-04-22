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

## Skill Contract

The overseer is implemented as a non-user-invocable skill named `verifying-completion-claims`.

Its description must make claim intent the trigger: use it automatically before any success, completion, passing, PR-ready, merged, or stage-complete statement.

The skill does not require slash-command integration. Agents invoke it because seeded migration project `CLAUDE.md` makes completion-claim verification mandatory.

The skill accepts free-form context instead of a command-specific schema. The agent supplies the intended claim, the work just performed, and any known run ID, object list, plan file, branch, PR, or validation command.

The skill returns claim wording guidance, not migration state. It tells the agent which claims are supported, which must be downgraded, and which are blocked until more evidence is gathered.

## Evidence Model

Before a completion claim, the overseer asks the agent to identify:

- the exact claim about to be made;
- the command, artifact, or state that proves the claim;
- whether the evidence is fresh from the current run;
- whether the agent read the output, exit code, artifact, or state directly; and
- whether the evidence supports the exact wording.

Common evidence includes run artifacts, summary files, catalog writeback, validated review JSON, dbt output, SQL comparison output, git state, PR state, and coordinator plan state.

## Skill Workflow

`verifying-completion-claims` follows a fixed evidence loop:

1. Identify the exact completion claim the agent intends to make.
2. Determine the minimum fresh evidence that would prove that claim.
3. Inspect that evidence directly.
4. Compare the evidence to the intended wording.
5. Return verified, downgraded, or blocked wording.

If the evidence is not already available, the skill instructs the agent what to inspect or run before making the claim. It must not infer success from confidence, elapsed workflow steps, expected side effects, or sub-agent summaries.

## Skill Output

The skill output should be concise and conversation-facing:

- `verified`: the exact claim is supported and may be stated;
- `downgraded`: only a narrower claim is supported, with replacement wording;
- `blocked`: no completion claim should be made, with the missing or contradictory evidence named.

The skill may include an evidence list when that helps the final user response stay precise.

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
