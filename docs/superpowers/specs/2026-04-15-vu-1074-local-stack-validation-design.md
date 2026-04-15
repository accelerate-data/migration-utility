# VU-1074 Local Stack Validation Spike Design

## Goal

Validate that the full CDO orchestration stack works locally before designing the production deployment: Paperclip starts and can dispatch tasks, `claude_local` and `codex_local` agents execute successfully, and agents can read and write Google Workspace documents via `gws`.

## Scope

This spike validates local agent execution only. Remote managed agent adapters, production credential strategy, Docker Compose stacks, and Azure deployment are all out of scope. Slack integration is deferred to the end of the spike with setup instructions provided; human operators update Paperclip tasks directly in the UI for the validation run.

## Non-goals

- Designing the production deployment (Azure, ACI, credential management)
- Building a `claude_managed` adapter for Anthropic's managed agents API
- OpenClaw — dropped from scope; `claude_local` and `codex_local` cover the execution layer
- Any CDO agent logic or migration orchestration

## Architecture

Paperclip is the control plane. It dispatches tasks to locally running agents via its built-in adapters. Agents call `gws` (a host CLI) via bash to interact with Google Workspace. Slack is deferred; human task updates replace it for this spike.

```text
Paperclip (localhost:3100)
  └── claude_local agent (cwd: ~/src/migration-utility)
        ├── Paperclip API  (read task, post comment, mark done)
        └── gws CLI        (create doc, write, read back)

  └── codex_local agent (optional, OpenRouter env vars)
        └── same tool surface
```

`claude_local` routes through Anthropic API directly. `codex_local` routes through OpenRouter when `OPENROUTER_API_KEY` is set in the agent's `env` config — no adapter changes required.

`gws auth login` must be run on the host before the validation run. Token is now valid.

## Setup Steps (human, one-time)

1. `cd ~/src/paperclip && pnpm dev` — start Paperclip; verify `http://localhost:3100` is accessible
2. In Paperclip UI: create a throwaway company and project for the spike
3. Register a `claude_local` agent with `cwd` set to `~/src/migration-utility`
4. Optionally register a `codex_local` agent with `env.OPENROUTER_API_KEY` set for cheaper model validation
5. Create one test task: *"VU-1074 spike validation"* and assign it to the claude_local agent

## Validation Run (approach: full chain first, debug what breaks)

Dispatch the test task. The agent instructions direct it to complete all steps in a single run:

1. Call Paperclip API to read its own assigned task and confirm task metadata is accessible
2. Call `gws drive files create` to create a Google Doc titled *"VU-1074 Spike Validation"*
3. Write a one-paragraph summary to the doc via `gws`
4. Call `gws drive files get` to read the doc back and verify the content matches what was written
5. Post a comment on the Paperclip task containing the Google Doc URL
6. Mark the Paperclip task as `done`

If any step fails, debug that step in isolation and re-run. No scaffolding — fix the failure and retry the full chain.

## Pass Criteria

- Paperclip task reaches `done` state
- Google Doc exists with correct written content
- Paperclip task has a comment containing the Doc URL

## Deferred: Slack Setup

Human updates Paperclip tasks directly in the UI while Slack is not yet wired. When ready, the Slack integration requires:

1. Create a Slack app at `https://api.slack.com/apps` with `chat:write`, `channels:read`, and `channels:history` bot token scopes
2. Install the app to your workspace and copy the bot token (`xoxb-...`)
3. Invite the bot to a throwaway channel
4. Add `@modelcontextprotocol/server-slack` to `.mcp.json` with `SLACK_BOT_TOKEN` and `SLACK_TEAM_ID` env vars
5. Validate: agent posts a message and reads a reply via the MCP tool

## Findings

All pass criteria met on 2026-04-15 using a `codex_local` agent.

- Paperclip starts cleanly via `pnpm dev` at `http://localhost:3100`
- `pnpm paperclipai onboard` is required on first run to generate `PAPERCLIP_AGENT_JWT_SECRET`; without it Paperclip cannot inject `PAPERCLIP_API_KEY` into agent runs and logs a warning
- `codex_local` agent with OpenRouter (`OPENROUTER_API_KEY`) dispatched and completed successfully
- `gws auth login` resolved the `invalid_rapt` token; all subsequent `gws` calls succeeded
- Agent created a Google Doc, wrote content, read it back, posted the Doc URL as a Paperclip task comment, and marked the task done
- Slack deferred; human updated Paperclip tasks directly in the UI with no issues

## Rejected Alternatives

### OpenClaw as execution runtime

Dropped because `claude_local` and `codex_local` adapters already exist in Paperclip and cover the local execution model without the added complexity of an OpenClaw Docker setup.

### Remote Claude managed agents

Deferred as a separate spike. Paperclip has no `claude_managed` adapter today, and building one is larger than a validation spike.

### Incremental service-by-service validation (Approach A)

Rejected in favour of Approach B (full chain first). The environment is mostly ready (`gws` is now authed, Paperclip starts cleanly), so a direct end-to-end run will surface real integration failures faster.
