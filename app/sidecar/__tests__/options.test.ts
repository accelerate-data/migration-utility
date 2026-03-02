import { describe, expect, it } from 'vitest';
import { buildQueryOptions } from '../options.ts';
import type { SidecarConfig } from '../config.ts';

function makeConfig(overrides: Partial<SidecarConfig> = {}): SidecarConfig {
  return {
    prompt: 'translate this proc',
    apiKey: 'sk-ant-test',
    cwd: '/tmp/migration',
    ...overrides,
  };
}

describe('buildQueryOptions', () => {
  it('uses agent-only when agentName is provided (no explicit model)', () => {
    const options = buildQueryOptions(makeConfig({ agentName: 'scope-table-details-analyzer' }), new AbortController());
    expect(options).toHaveProperty('agent', 'scope-table-details-analyzer');
    expect(options).not.toHaveProperty('model');
    expect(options.settingSources).toEqual(['project']);
  });

  it('uses explicit model when provided without agentName', () => {
    const options = buildQueryOptions(makeConfig({ model: 'claude-sonnet-4-6' }), new AbortController());
    expect(options).toHaveProperty('model', 'claude-sonnet-4-6');
    expect(options).not.toHaveProperty('agent');
  });

  it('passes both agent and model when both are provided', () => {
    const options = buildQueryOptions(
      makeConfig({
        agentName: 'scope-table-details-analyzer',
        model: 'claude-sonnet-4-6',
      }),
      new AbortController(),
    );
    expect(options).toHaveProperty('agent', 'scope-table-details-analyzer');
    expect(options).toHaveProperty('model', 'claude-sonnet-4-6');
  });

  it('keeps project setting source', () => {
    const options = buildQueryOptions(makeConfig({ model: 'claude-sonnet-4-6' }), new AbortController());
    expect(options.settingSources).toEqual(['project']);
  });

  it('includes preset claude_code system prompt', () => {
    const options = buildQueryOptions(makeConfig(), new AbortController());
    expect(options.systemPrompt).toEqual({ type: 'preset', preset: 'claude_code' });
  });

  it('uses cwd and env api key', () => {
    const options = buildQueryOptions(makeConfig({ cwd: '/tmp/work', apiKey: 'sk-ant-2' }), new AbortController());
    expect(options.cwd).toBe('/tmp/work');
    expect((options.env ?? {}).ANTHROPIC_API_KEY).toBe('sk-ant-2');
  });

  it('uses bypassPermissions as default permission mode', () => {
    const options = buildQueryOptions(makeConfig(), new AbortController());
    expect(options.permissionMode).toBe('bypassPermissions');
  });
});
