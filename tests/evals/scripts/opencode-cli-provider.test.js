const assert = require('node:assert/strict');
const path = require('node:path');
const test = require('node:test');

const OpenCodeCliProvider = require('./opencode-cli-provider');

test('OpenCodeCliProvider invokes opencode run with the configured Qwen model', async () => {
  const calls = [];
  const provider = new OpenCodeCliProvider({
    config: {
      provider_id: 'opencode',
      model: 'qwen3.6-plus',
      working_dir: '..',
    },
    runner: async (args, options) => {
      calls.push({ args, options });
      return 'status output';
    },
  });

  const result = await provider.callApi('run status');

  assert.deepEqual(result, { output: 'status output' });
  assert.deepEqual(calls[0].args, [
    'run',
    '--model',
    'opencode/qwen3.6-plus',
    '--agent',
    'build',
    'run status',
  ]);
  assert.equal(calls[0].options.cwd, path.resolve(__dirname, '..', '..'));
  assert.match(calls[0].options.env.XDG_STATE_HOME, /\.promptfoo\/opencode-runtime\/state$/);
});

test('OpenCodeCliProvider retries empty CLI output when configured', async () => {
  let calls = 0;
  const provider = new OpenCodeCliProvider({
    config: {
      provider_id: 'opencode',
      model: 'qwen3.6-plus',
      empty_output_retries: 1,
    },
    runner: async () => {
      calls += 1;
      return calls === 1 ? '   ' : 'usable output';
    },
  });

  const result = await provider.callApi('prompt');

  assert.deepEqual(result, { output: 'usable output' });
  assert.equal(calls, 2);
});

test('OpenCodeCliProvider reports empty output after configured retries', async () => {
  const provider = new OpenCodeCliProvider({
    config: {
      provider_id: 'opencode',
      model: 'qwen3.6-plus',
      empty_output_retries: 1,
    },
    runner: async () => '',
  });

  const result = await provider.callApi('prompt');

  assert.deepEqual(result, {
    error: 'OpenCode CLI returned empty output after 2 attempt(s)',
  });
});

test('OpenCodeCliProvider validates required model config and retry count', async () => {
  const missingModel = new OpenCodeCliProvider({
    config: {
      provider_id: 'opencode',
    },
  });

  assert.deepEqual(await missingModel.callApi('prompt'), {
    error: 'OpenCode CLI provider requires provider_id and model',
  });

  const invalidRetries = new OpenCodeCliProvider({
    config: {
      provider_id: 'opencode',
      model: 'qwen3.6-plus',
      empty_output_retries: -1,
    },
    runner: async () => 'unused',
  });

  assert.deepEqual(await invalidRetries.callApi('prompt'), {
    error: 'OpenCode CLI provider requires empty_output_retries to be a non-negative integer',
  });
});
