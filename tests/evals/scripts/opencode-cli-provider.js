const { spawn } = require('node:child_process');
const path = require('node:path');

const EVAL_ROOT = path.resolve(__dirname, '..');
const DEFAULT_STATE_HOME = path.join(EVAL_ROOT, '.promptfoo', 'opencode-runtime', 'state');

class OpenCodeCliProvider {
  constructor(options = {}) {
    this.config = options.config || {};
    this.providerId = options.id || 'opencode:cli';
    this.runner = options.runner || runOpenCode;
  }

  id() {
    return this.providerId;
  }

  async callApi(prompt, _context, callOptions = {}) {
    const providerId = this.config.provider_id;
    const model = this.config.model;
    if (!providerId || !model) {
      return { error: 'OpenCode CLI provider requires provider_id and model' };
    }

    try {
      const output = await this.callWithEmptyOutputRetries(prompt, providerId, model, callOptions);
      return { output };
    } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
    }
  }

  async callWithEmptyOutputRetries(prompt, providerId, model, callOptions) {
    const maxAttempts = 1 + normalizeRetryCount(this.config.empty_output_retries);
    let attempt = 0;

    while (attempt < maxAttempts) {
      attempt += 1;
      const output = await this.runner([
        'run',
        '--model',
        `${providerId}/${model}`,
        '--agent',
        this.config.agent || 'build',
        prompt,
      ], {
        cwd: path.resolve(EVAL_ROOT, this.config.working_dir || '.'),
        env: {
          ...process.env,
          XDG_STATE_HOME: process.env.XDG_STATE_HOME || DEFAULT_STATE_HOME,
        },
        signal: callOptions.abortSignal,
      });
      const trimmedOutput = output.trim();
      if (trimmedOutput) {
        return trimmedOutput;
      }
    }

    throw new Error(`OpenCode CLI returned empty output after ${maxAttempts} attempt(s)`);
  }
}

function normalizeRetryCount(value) {
  if (value === undefined) {
    return 0;
  }

  if (!Number.isInteger(value) || value < 0) {
    throw new Error('OpenCode CLI provider requires empty_output_retries to be a non-negative integer');
  }

  return value;
}

function runOpenCode(args, options) {
  return new Promise((resolve, reject) => {
    const child = spawn('opencode', args, {
      cwd: options.cwd,
      env: options.env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const stdout = [];
    const stderr = [];

    const abort = () => {
      child.kill('SIGTERM');
      reject(new Error('OpenCode CLI call aborted'));
    };

    if (options.signal) {
      if (options.signal.aborted) {
        abort();
        return;
      }
      options.signal.addEventListener('abort', abort, { once: true });
    }

    child.stdout.on('data', (chunk) => stdout.push(chunk));
    child.stderr.on('data', (chunk) => stderr.push(chunk));
    child.on('error', reject);
    child.on('close', (code) => {
      if (options.signal) {
        options.signal.removeEventListener('abort', abort);
      }

      const output = Buffer.concat(stdout).toString('utf8');
      const errorOutput = Buffer.concat(stderr).toString('utf8').trim();
      if (code === 0) {
        resolve(output);
        return;
      }

      reject(new Error(errorOutput || `opencode exited with code ${code}`));
    });
  });
}

module.exports = OpenCodeCliProvider;
module.exports.runOpenCode = runOpenCode;
