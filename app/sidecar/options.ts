import type { SDKSessionOptions } from '@anthropic-ai/claude-agent-sdk';
import type { SidecarConfig } from './config.ts';

const DEFAULT_MODEL = 'claude-sonnet-4-6';

// V2 SDK types currently omit some fields we need from existing runtime behavior.
type ExtendedSessionOptions = SDKSessionOptions & {
  agent?: string;
  model: string;
  cwd: string;
  settingSources: Array<'project'>;
  systemPrompt: { type: 'preset'; preset: 'claude_code' };
  permissionMode: 'bypassPermissions';
  allowDangerouslySkipPermissions: boolean;
};

export function buildSessionOptions(config: SidecarConfig): ExtendedSessionOptions {
  const agentField = config.agentName?.trim() ? { agent: config.agentName.trim() } : {};
  // SDKSessionOptions requires a model; keep global default unless caller overrides.
  const model = config.model?.trim() || DEFAULT_MODEL;
  return {
    ...agentField,
    model,
    env: {
      ...process.env,
      ANTHROPIC_API_KEY: config.apiKey,
    },
    // Required parity with existing runtime behavior
    settingSources: ['project'],
    systemPrompt: { type: 'preset', preset: 'claude_code' },
    cwd: config.cwd,
    // Non-interactive migration runs should never wait on permission prompts.
    permissionMode: 'bypassPermissions',
    allowDangerouslySkipPermissions: true,
    executable: 'node',
  };
}

export function buildInitialPrompt(config: SidecarConfig): string {
  const system = config.systemPrompt?.trim();
  if (!system) return config.prompt;
  return `${system}\n\n${config.prompt}`;
}
