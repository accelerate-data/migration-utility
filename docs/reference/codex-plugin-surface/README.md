# Codex Plugin Surface

`ad-migration` publishes both Claude and Codex plugin manifests. The manifest versions move together with the Python package versions because all release-facing plugin surfaces are validated as one deployable unit.

## Supported Codex Surfaces

The Codex manifest supports root `skills/` through the `skills` field. These skill directories are shared with the Claude plugin and remain the source of truth for migration workflow instructions.

The Codex manifest supports the root `.mcp.json` through the `mcpServers` field. This exposes the bundled local DDL MCP server only; live database tool configuration under `.claude/mcp/tools.yaml` stays Claude-only/local-only because it depends on Claude-specific genai-toolbox wiring and local environment variables.

## Claude-Only Surfaces

Root `commands/` are intentionally Claude-only. They are slash-command specs that rely on Claude Code command semantics and stage-helper conventions, and this repository does not currently have a Codex command-runtime contract to map them to without changing behavior.

Do not add or rename plugin-facing surfaces without updating `.claude-plugin/plugin.json`, `.codex-plugin/plugin.json`, `scripts/validate_plugin_manifests.py`, `scripts/check_version_consistency.py`, `repo-map.json`, and the relevant docs in the same change.
