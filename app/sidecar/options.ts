import type { SDKSessionOptions } from '@anthropic-ai/claude-agent-sdk';
import type { SidecarConfig } from './config.ts';

const DEFAULT_MODEL = 'claude-sonnet-4-6';

// V2 SDK types currently omit some fields we need from existing runtime behavior.
export type SessionOptionsWithOptionalModel = Omit<SDKSessionOptions, 'model'> & {
  agent?: string;
  model?: string;
  cwd: string;
  settingSources: Array<'project'>;
  systemPrompt: { type: 'preset'; preset: 'claude_code' };
  permissionMode: 'bypassPermissions';
  allowDangerouslySkipPermissions: boolean;
};

export function buildSessionOptions(config: SidecarConfig): SessionOptionsWithOptionalModel {
  const hasAgent = Boolean(config.agentName?.trim());
  const agentField = hasAgent ? { agent: config.agentName!.trim() } : {};
  const explicitModel = config.model?.trim();
  const modelField = explicitModel ? { model: explicitModel } : hasAgent ? {} : { model: DEFAULT_MODEL };
  return {
    ...agentField,
    ...modelField,
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

export function redactSessionOptionsForLog(options: SessionOptionsWithOptionalModel): Record<string, unknown> {
  return {
    ...options,
    env: {
      ...(options.env ?? {}),
      ANTHROPIC_API_KEY: '[REDACTED]',
    },
  };
}

export function buildInitialPrompt(config: SidecarConfig): string {
  const system = config.systemPrompt?.trim();
  if (!system) return config.prompt;
  return `${system}\n\n${config.prompt}`;
}
