@AGENTS.md

## Adapter Role

`AGENTS.md` is canonical for repository-wide guidance. This file is an adapter for Claude-specific routing and should stay lightweight.

## Delegation Policy

### Model tiers

| Tier | Model | When |
|---|---|---|
| Reasoning | sonnet | Planning, architecture, requirements drafting |
| Implementation | default | Coding, exploration, review, merge |
| Lightweight | haiku | Linear API calls, status updates, simple lookups |

### Sub-agent rules

Commit + push before reporting completion.