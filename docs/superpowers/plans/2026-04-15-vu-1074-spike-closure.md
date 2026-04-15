# VU-1074 Spike Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out the VU-1074 local stack validation spike — update the spec with confirmed findings, create a follow-on Linear issue for Slack, and mark the spike done.

**Architecture:** Spike is already validated. All pass criteria met (Paperclip running, codex_local agent dispatched, gws created/wrote/read a Google Doc, Paperclip task commented and marked done). This plan covers documentation closure and follow-on issue creation only.

**Tech Stack:** git, Linear MCP, markdownlint

---

## Task 1: Update spec with confirmed findings

The spec was written before the run. Update it to reflect what actually happened.

**Files:**

- Modify: `docs/superpowers/specs/2026-04-15-vu-1074-local-stack-validation-design.md`

- [ ] **Step 1: Add a Findings section to the spec**

Append the following section before `## Rejected Alternatives`:

```markdown
## Findings

All pass criteria met on 2026-04-15 using a `codex_local` agent.

- Paperclip starts cleanly via `pnpm dev` at `http://localhost:3100`
- `pnpm paperclipai onboard` is required on first run to generate `PAPERCLIP_AGENT_JWT_SECRET`; without it Paperclip cannot inject `PAPERCLIP_API_KEY` into agent runs and logs a warning
- `codex_local` agent with OpenRouter (`OPENROUTER_API_KEY`) dispatched and completed successfully
- `gws auth login` resolved the `invalid_rapt` token; all subsequent `gws` calls succeeded
- Agent created a Google Doc, wrote content, read it back, posted the Doc URL as a Paperclip task comment, and marked the task done
- Slack deferred; human updated Paperclip tasks directly in the UI with no issues
```

- [ ] **Step 2: Run markdownlint**

```bash
markdownlint docs/superpowers/specs/2026-04-15-vu-1074-local-stack-validation-design.md
```

Expected: no output (clean).

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/specs/2026-04-15-vu-1074-local-stack-validation-design.md
git commit -m "docs: update VU-1074 spec with confirmed spike findings"
```

---

## Task 2: Create follow-on Linear issue for Slack integration

The Slack MCP setup is validated-by-design but not yet executed. Capture it as a follow-on spike.

**Files:** none (Linear MCP call only)

- [ ] **Step 1: Create the Linear issue**

Use the Linear MCP tool to create the issue in the Utilities team, Warehouse Migration project, with the following fields:

- **Title:** `Spike: Wire Slack MCP server for CDO agent comms`
- **Estimate:** S (2)
- **Label:** spike
- **Project:** Warehouse Migration
- **Description:**

```markdown
## Problem

Slack async comms are not yet wired for CDO agent runs. Human operators currently update Paperclip tasks directly in the UI. VU-1074 validated the rest of the local stack; this spike completes the Slack leg.

## Steps

1. Create a Slack app at https://api.slack.com/apps with scopes: `chat:write`, `channels:read`, `channels:history`
2. Install the app to the workspace and copy the bot token (`xoxb-...`)
3. Invite the bot to a throwaway channel
4. Add `@modelcontextprotocol/server-slack` to `.mcp.json`:

\`\`\`json
"slack": {
  "command": "npx",
  "args": ["-y", "@modelcontextprotocol/server-slack"],
  "env": {
    "SLACK_BOT_TOKEN": "${SLACK_BOT_TOKEN}",
    "SLACK_TEAM_ID": "${SLACK_TEAM_ID}"
  }
}
\`\`\`

5. Assign a Paperclip task to a `claude_local` agent; confirm the agent can post a message and read a reply via the MCP tool

## Acceptance Criteria

- [ ] Slack MCP server running and registered in `.mcp.json`
- [ ] Agent posts a message to a Slack channel via MCP tool
- [ ] Agent reads a reply from the channel via MCP tool
- [ ] No bot token committed to the repo
```

- [ ] **Step 2: Confirm the issue was created and note its ID**

Log the new issue identifier (e.g. `VU-XXXX`) for reference.

---

## Task 3: Mark VU-1074 done in Linear

- [ ] **Step 1: Update VU-1074 status to Done**

Use the Linear MCP tool `mcp__claude_ai_Linear__save_issue` with `id: "VU-1074"` and `stateId` set to the Done state for the Utilities team.

- [ ] **Step 2: Push the spec update commit**

```bash
git push
```

Expected: commit from Task 1 pushed to `main`.
