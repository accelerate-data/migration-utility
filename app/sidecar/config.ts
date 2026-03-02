export interface SidecarConfig {
  prompt: string;
  model?: string;
  agentName?: string;
  apiKey: string;
  cwd: string;
  allowedTools?: string[];
  maxTurns?: number;
  permissionMode?: string;
  sessionId?: string;
  betas?: string[];
  maxThinkingTokens?: number;
  pathToClaudeCodeExecutable?: string;
}

/**
 * Runtime-validate an unknown value into a SidecarConfig.
 * Replaces unsafe `as SidecarConfig` casts in persistent-mode.
 */
export function parseSidecarConfig(raw: unknown): SidecarConfig {
  if (typeof raw !== "object" || raw === null) {
    throw new Error("Invalid SidecarConfig: expected object");
  }
  const c = raw as Record<string, unknown>;
  if (typeof c.prompt !== "string") throw new Error("Invalid SidecarConfig: missing prompt");
  if (typeof c.apiKey !== "string" || c.apiKey.length === 0) throw new Error("Invalid SidecarConfig: missing apiKey");
  if (typeof c.cwd !== "string") throw new Error("Invalid SidecarConfig: missing cwd");
  return raw as SidecarConfig;
}
