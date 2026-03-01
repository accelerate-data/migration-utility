import type { SDKSessionOptions } from '@anthropic-ai/claude-agent-sdk';
import type { SidecarConfig } from './config.ts';

const DEFAULT_MODEL = 'claude-sonnet-4-6';

// V2 SDK types currently omit some fields we need from existing runtime behavior.
type ExtendedSessionOptions = SDKSessionOptions & {
  agent?: string;
  model?: string;
  cwd: string;
  settingSources: Array<'project'>;
  systemPrompt: { type: 'preset'; preset: 'claude_code' };
  permissionMode: 'bypassPermissions';
  allowDangerouslySkipPermissions: boolean;
};

export function buildSessionOptions(config: SidecarConfig): ExtendedSessionOptions {
  const agentField = config.agentName?.trim() ? { agent: config.agentName.trim() } : {};
  // Mirror skill-builder behavior:
  // - agentName only  -> use agent/frontmatter model
  // - model only      -> use provided model
  // - both            -> model overrides frontmatter
  const modelField =
    config.model?.trim()
      ? { model: config.model.trim() }
      : config.agentName?.trim()
        ? {}
        : { model: DEFAULT_MODEL };
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

export function buildInitialPrompt(config: SidecarConfig): string {
  const system = config.systemPrompt?.trim();
  if (!system) return config.prompt;
  return `${system}\n\n${config.prompt}`;
}
