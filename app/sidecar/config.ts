export interface SidecarConfig {
  prompt: string;
  model?: string;
  agentName?: string;
  allowedTools?: string[];
  apiKey: string;
  cwd: string;
  systemPrompt?: string;
}

export function parseSidecarConfig(raw: unknown): SidecarConfig {
  if (typeof raw !== 'object' || raw === null) {
    throw new Error('Invalid SidecarConfig: expected object');
  }
  const c = raw as Record<string, unknown>;
  if (typeof c.prompt !== 'string' || c.prompt.trim().length === 0) {
    throw new Error('Invalid SidecarConfig: missing prompt');
  }
  if (typeof c.apiKey !== 'string' || c.apiKey.trim().length === 0) {
    throw new Error('Invalid SidecarConfig: missing apiKey');
  }
  if (typeof c.cwd !== 'string' || c.cwd.trim().length === 0) {
    throw new Error('Invalid SidecarConfig: missing cwd');
  }
  return {
    prompt: c.prompt,
    model: typeof c.model === 'string' ? c.model : undefined,
    agentName: typeof c.agentName === 'string' ? c.agentName : undefined,
    allowedTools: Array.isArray(c.allowedTools) ? (c.allowedTools as string[]) : undefined,
    apiKey: c.apiKey,
    cwd: c.cwd,
    systemPrompt: typeof c.systemPrompt === 'string' ? c.systemPrompt : undefined,
  };
}
