import { describe, expect, it } from 'vitest';
import { buildInitialPrompt, buildSessionOptions } from '../options.ts';
import type { SidecarConfig } from '../config.ts';

function makeConfig(overrides: Partial<SidecarConfig> = {}): SidecarConfig {
  return {
    prompt: 'translate this proc',
    apiKey: 'sk-ant-test',
    cwd: '/tmp/migration',
    ...overrides,
  };
}

describe('buildSessionOptions', () => {
  it('uses agent-only when agentName is provided (no explicit model)', () => {
    const options = buildSessionOptions(makeConfig({ agentName: 'scope-table-details-analyzer' }));
    expect(options).toHaveProperty('agent', 'scope-table-details-analyzer');
    expect(options).not.toHaveProperty('model');
    expect(options.settingSources).toEqual(['project']);
  });

  it('uses explicit model when provided without agentName', () => {
    const options = buildSessionOptions(makeConfig({ model: 'claude-sonnet-4-6' }));
    expect(options).toHaveProperty('model', 'claude-sonnet-4-6');
    expect(options).not.toHaveProperty('agent');
  });

  it('passes both agent and model when both are provided', () => {
    const options = buildSessionOptions(
      makeConfig({
        agentName: 'scope-table-details-analyzer',
        model: 'claude-sonnet-4-6',
      }),
    );
    expect(options).toHaveProperty('agent', 'scope-table-details-analyzer');
    expect(options).toHaveProperty('model', 'claude-sonnet-4-6');
  });

  it('falls back to default model when neither agentName nor model is provided', () => {
    const options = buildSessionOptions(makeConfig());
    expect(options).toHaveProperty('model', 'claude-sonnet-4-6');
  });

  it('keeps project setting source', () => {
    const options = buildSessionOptions(makeConfig({ model: 'claude-sonnet-4-6' }));
    expect(options.settingSources).toEqual(['project']);
    expect(options).not.toHaveProperty('systemPrompt');
  });

  it('uses cwd and env api key', () => {
    const options = buildSessionOptions(makeConfig({ cwd: '/tmp/work', apiKey: 'sk-ant-2' }));
    expect(options.cwd).toBe('/tmp/work');
    expect((options.env ?? {}).ANTHROPIC_API_KEY).toBe('sk-ant-2');
  });
});

describe('buildInitialPrompt', () => {
  it('returns prompt as-is when no system prompt', () => {
    expect(buildInitialPrompt(makeConfig())).toBe('translate this proc');
  });

  it('prepends system prompt when provided', () => {
    expect(buildInitialPrompt(makeConfig({ systemPrompt: 'You are a dbt expert' }))).toBe(
      'You are a dbt expert\n\ntranslate this proc',
    );
  });
});
