const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const providerDir = __dirname;

function allowedTools(providerFile) {
  const text = fs.readFileSync(path.join(providerDir, providerFile), 'utf8');
  const tools = [];
  let inTools = false;

  for (const line of text.split('\n')) {
    if (/^\s*append_allowed_tools:\s*$/.test(line)) {
      inTools = true;
      continue;
    }

    if (inTools && /^\s+-\s+/.test(line)) {
      tools.push(line.replace(/^\s+-\s+/, '').trim());
      continue;
    }

    if (inTools && /^\S/.test(line)) {
      break;
    }
  }

  return tools;
}

test('long-running Claude agent providers allow in-place file edits', () => {
  for (const providerFile of ['haiku-100.yaml', 'haiku-120.yaml', 'sonnet-120.yaml', 'sonnet-200.yaml']) {
    assert.ok(
      allowedTools(providerFile).includes('Edit'),
      `${providerFile} must allow Edit so skills can update existing plan and artifact files`,
    );
  }
});
