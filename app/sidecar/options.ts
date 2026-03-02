import type { Options } from "@anthropic-ai/claude-agent-sdk";
import type { SidecarConfig } from "./config.ts";

/**
 * Build the options object to pass to the SDK query() function.
 *
 * Agent / model resolution (settingSources: ['project'] always passed for project settings):
 *  - agentName only  → agent (front-matter model used)
 *  - model only      → model
 *  - both            → agent + model (model overrides front-matter)
 */
export function buildQueryOptions(
  config: SidecarConfig,
  abortController: AbortController,
  stderr?: (data: string) => void,
) {
  // --- agent / model resolution ---
  const agentField = config.agentName ? { agent: config.agentName } : {};

  // When model is set, always pass it — whether it's the sole identifier
  // (model-only) or overriding the agent's front-matter model (both).
  const modelField = config.model ? { model: config.model } : {};

  // Pass the API key through the SDK's env option instead of mutating
  // process.env, which avoids races on concurrent requests.
  const envField = config.apiKey
    ? { env: { ...process.env, ANTHROPIC_API_KEY: config.apiKey } }
    : {};

  return {
    ...agentField,
    ...modelField,
    ...envField,
    // Include the full Claude Code system prompt so the model knows how to
    // use tools (Read, Write, Bash, Skill, etc.) and follows CC conventions.
    systemPrompt: { type: 'preset' as const, preset: 'claude_code' as const },
    // Load project settings (skill discovery, CLAUDE.md) from {cwd}/.claude/.
    // 'user' is intentionally excluded — it causes the SDK to scan
    // ~/.claude/skills/ (wasted reads) and the sidecar can't use the
    // user's MCP servers anyway (those are CLI-process-only).
    settingSources: ['project' as const],
    cwd: config.cwd,
    // Whitelist filter over the loaded tool set. For agent runs, Rust extracts
    // allowedTools from the agent's front-matter and includes it in the payload;
    // forwarding it here enforces the front-matter tool policy at the SDK level.
    ...(config.allowedTools ? { allowedTools: config.allowedTools } : {}),
    // Suppress all MCP server loading. The agent cwd may have project settings
    // that reference user claude.ai MCP integrations (Gmail, Slack, Linear, etc.)
    // which are irrelevant to analysis agents and inflate the tool list.
    mcpServers: {},
    maxTurns: config.maxTurns ?? 50,
    permissionMode: (config.permissionMode || "bypassPermissions") as
      | "default"
      | "acceptEdits"
      | "bypassPermissions"
      | "plan",
    // Required by SDK when using permissionMode: 'bypassPermissions'.
    allowDangerouslySkipPermissions: true,
    // Analysis agents are ephemeral — no need to persist session state to disk.
    persistSession: false,
    abortController,
    // Use the same Node binary that's running this sidecar process,
    // so the SDK spawns cli.js with a compatible Node version.
    executable: process.execPath as 'node',
    ...(config.pathToClaudeCodeExecutable
      ? { pathToClaudeCodeExecutable: config.pathToClaudeCodeExecutable }
      : {}),
    ...(config.sessionId ? { resume: config.sessionId } : {}),
    ...(config.betas ? { betas: config.betas as Options['betas'] } : {}),
    ...(config.maxThinkingTokens ? { maxThinkingTokens: config.maxThinkingTokens } : {}),
    ...(config.effort ? { effort: config.effort } : {}),
    ...(config.debugFile ? { debugFile: config.debugFile } : {}),
    ...(config.debug ? { debug: config.debug } : {}),
    ...(stderr ? { stderr } : {}),
  };
}
