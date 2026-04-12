# Linear Operations

Execute Linear operations directly by default using MCP tools. Use sub-agents only when parallel research is required.

## Required MCP Tools

- `mcp__linear__list_issues`
- `mcp__linear__get_issue`
- `mcp__linear__list_projects`
- `mcp__linear__get_project`
- `mcp__linear__list_milestones`
- `mcp__linear__list_issue_labels`
- `mcp__linear__save_issue`
- `mcp__linear__save_comment`

If any required tool fails after one retry, stop and report the exact failing step.
