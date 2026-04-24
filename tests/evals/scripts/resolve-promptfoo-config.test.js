const assert = require('node:assert/strict');
const test = require('node:test');
const path = require('node:path');

const {
  TMP_ROOT,
  resolveConfigFile,
  resolveProviderId,
  writeResolvedConfig,
} = require('./resolve-promptfoo-config');

test('resolveConfigFile materializes an opencode provider from metadata.eval_tier', () => {
  const resolved = resolveConfigFile('packages/listing-objects/skill-listing-objects.yaml');

  assert.equal(
    resolved.providers[0].id,
    `file://${path.join(path.dirname(TMP_ROOT), '..', 'scripts', 'opencode-cli-provider.js')}`,
  );
  assert.equal(resolved.providers[0].config.agent, 'eval_light');
  assert.equal(resolved.providers[0].config.format, 'default');
  assert.equal(resolved.providers[0].config.log_level, 'ERROR');
  assert.equal(resolved.providers[0].config.print_logs, false);
  assert.equal(resolved.providers[0].config.empty_output_retries, 1);
  assert.match(resolved.providers[0].config.opencode_config, /tests\/evals\/opencode\.json$/);
  assert.equal(resolved.providers[0].config.project_dir, path.resolve(__dirname, '..', '..', '..'));
  assert.equal('max_turns' in resolved.providers[0].config, false);
  assert.equal('model' in resolved.providers[0].config, false);
  assert.equal('provider_id' in resolved.providers[0].config, false);
  assert.equal('tools' in resolved.providers[0].config, false);
  assert.equal(
    resolved.prompts[0].raw,
    'file://../../../../prompts/skill-listing-objects.txt',
  );
  assert.equal(
    resolved.extensions[0],
    'file://../../../../scripts/run-workspace-extension.js:extensionHook',
  );
});

test('resolveConfigFile rejects configs missing metadata.eval_tier', () => {
  const relativePath = 'scripts/fixtures/missing-eval-tier.yaml';

  assert.throws(
    () => resolveConfigFile(relativePath),
    new RegExp(`${relativePath.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')} is missing metadata\\.eval_tier`),
  );
});

test('resolveConfigFile rejects traversal outside the eval root', () => {
  assert.throws(
    () => resolveConfigFile('packages/../../foo.yaml'),
    /Refusing to access config outside eval root: packages\/\.\.\/\.\.\/foo\.yaml/,
  );
});

test('writeResolvedConfig writes suite-owned resolved configs only under .tmp', () => {
  const calls = [];
  const relativePath = writeResolvedConfig(
    'packages/listing-objects/skill-listing-objects.yaml',
    {
      fsImpl: {
        mkdirSync: (targetPath, options) => {
          calls.push(['mkdir', targetPath, options]);
        },
        writeFileSync: (targetPath, contents, encoding) => {
          calls.push(['write', targetPath, contents, encoding]);
        },
      },
    },
  );

  assert.match(relativePath, /^\.tmp\/resolved-configs\/packages\/listing-objects\/skill-listing-objects\.yaml$/);
  assert.deepEqual(calls[0], ['mkdir', TMP_ROOT, { recursive: true }]);
  assert.deepEqual(calls[1], [
    'mkdir',
    path.join(TMP_ROOT, 'packages', 'listing-objects'),
    { recursive: true },
  ]);
  assert.equal(calls[2][0], 'write');
  assert.equal(calls[2][1], path.join(TMP_ROOT, 'packages', 'listing-objects', 'skill-listing-objects.yaml'));
  assert.match(calls[2][2], /file:\/\/.*\/scripts\/opencode-cli-provider\.js/);
  assert.equal(calls[2][3], 'utf8');
});

test('resolveProviderId makes local file providers stable from materialized configs', () => {
  assert.equal(
    resolveProviderId('file://scripts/opencode-cli-provider.js'),
    `file://${path.join(path.dirname(TMP_ROOT), '..', 'scripts', 'opencode-cli-provider.js')}`,
  );
  assert.equal(resolveProviderId('custom:provider'), 'custom:provider');
});

test('writeResolvedConfig rejects traversal outside the resolved-config output root', () => {
  assert.throws(
    () => writeResolvedConfig(
      'packages/listing-objects/skill-listing-objects.yaml',
      {
        fsImpl: {
          mkdirSync: () => {
            throw new Error('should not mkdir');
          },
          writeFileSync: () => {
            throw new Error('should not write');
          },
        },
        outputRoot: path.join(TMP_ROOT, '..'),
      },
    ),
    /Refusing to write resolved configs outside \.tmp\/resolved-configs/,
  );
});
