/**
 * Integration test: real sidecar + real Claude API.
 *
 * Requires:
 *   - ANTHROPIC_API_KEY env var  OR  .claude/settings.local with { "env": { "ANTHROPIC_API_KEY": "..." } }
 *   - Built sidecar: `npm run sidecar:build` from app/
 */
import { describe, it, expect } from 'vitest';
import { spawn } from 'child_process';
import { resolve, dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { existsSync, mkdirSync, copyFileSync, readdirSync, readFileSync, rmSync } from 'fs';
import { tmpdir } from 'os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const sidecarEntry = resolve(__dirname, '..', 'dist', 'index.js');
// 3 levels up from app/sidecar/__tests__ → repo root
const worktreeRoot = resolve(__dirname, '..', '..', '..');
const agentSourcesRoot = join(worktreeRoot, 'agent-sources', 'workspace');

function readApiKey(): string | null {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  const settingsPath = join(worktreeRoot, '.claude', 'settings.local');
  if (existsSync(settingsPath)) {
    try {
      const s = JSON.parse(readFileSync(settingsPath, 'utf-8'));
      return (s as { env?: { ANTHROPIC_API_KEY?: string } })?.env?.ANTHROPIC_API_KEY ?? null;
    } catch { return null; }
  }
  return null;
}

/** Set up a temp workspace mirroring what the SDK expects:
 *  {cwd}/CLAUDE.md                                  ← project instructions (root)
 *  {cwd}/.claude/rules/*.md                         ← auto-loaded source rules
 *  {cwd}/.claude/agents/scope-table-details-analyzer.md
 *  {cwd}/.claude/skills/classify-source-object/SKILL.md
 */
function setupWorkspace(): string {
  const dir = join(tmpdir(), `sidecar-int-${Date.now()}`);
  mkdirSync(join(dir, '.claude', 'agents'), { recursive: true });
  mkdirSync(join(dir, '.claude', 'skills', 'classify-source-object'), { recursive: true });
  mkdirSync(join(dir, '.claude', 'rules'), { recursive: true });

  // CLAUDE.md at cwd root — SDK auto-loads from here
  copyFileSync(
    join(agentSourcesRoot, 'CLAUDE.md'),
    join(dir, 'CLAUDE.md'),
  );

  // Source rules — auto-loaded from .claude/rules/
  for (const file of readdirSync(join(agentSourcesRoot, 'rules'))) {
    copyFileSync(
      join(agentSourcesRoot, 'rules', file),
      join(dir, '.claude', 'rules', file),
    );
  }

  copyFileSync(
    join(agentSourcesRoot, 'agents', 'scope-table-details-analyzer.md'),
    join(dir, '.claude', 'agents', 'scope-table-details-analyzer.md'),
  );
  copyFileSync(
    join(agentSourcesRoot, 'skills', 'classify-source-object', 'SKILL.md'),
    join(dir, '.claude', 'skills', 'classify-source-object', 'SKILL.md'),
  );
  return dir;
}

const apiKey = readApiKey();
const canRun = !!apiKey && existsSync(sidecarEntry) && existsSync(agentSourcesRoot);

describe.skipIf(!canRun)('sidecar integration', () => {
  it('scope-table-details-analyzer returns JSON without extra tool calls', async () => {
    const cwd = setupWorkspace();

    try {
      // Unset CLAUDECODE so the SDK doesn't refuse to run inside a Claude Code session
      const env = { ...process.env };
      delete env['CLAUDECODE'];
      const child = spawn('node', [sidecarEntry], { stdio: 'pipe', env });
      const messages: Record<string, unknown>[] = [];

      child.stdout.on('data', (data: Buffer) => {
        for (const line of data.toString().split('\n').filter(Boolean)) {
          try {
            const msg = JSON.parse(line) as Record<string, unknown>;
            messages.push(msg);
            // Print each message as it arrives for real-time visibility
            process.stderr.write(`[msg] ${JSON.stringify(msg)}\n`);
          } catch {
            process.stderr.write(`[stdout-raw] ${line}\n`);
          }
        }
      });
      child.stderr.on('data', (d: Buffer) => {
        process.stderr.write(`[sidecar-stderr] ${d.toString()}`);
      });

      // Wait for sidecar_ready
      await new Promise<void>((res, rej) => {
        const t = setTimeout(() => rej(new Error('sidecar_ready timeout')), 15_000);
        const iv = setInterval(() => {
          if (messages.some((m) => m['type'] === 'sidecar_ready')) {
            clearInterval(iv); clearTimeout(t); res();
          }
        }, 50);
      });

      const prompt = [
        'Analyze table details for migration metadata.',
        'Return exactly one JSON object following the contract.',
        'CONTEXT_START',
        'workspace_id: test-ws-1',
        'selected_table_id: test-table-1',
        'schema_name: dbo',
        'table_name: Orders',
        'columns: [{"name":"id","type":"int","is_nullable":false},{"name":"customer_id","type":"int","is_nullable":false},{"name":"amount","type":"decimal","is_nullable":true}]',
        'primary_keys: ["id"]',
        'foreign_keys: [{"child_column":"customer_id","parent_table":"Customers","parent_column":"id"}]',
        'row_count: 50000',
        'sp_body: ',
        'CONTEXT_END',
      ].join('\n');

      const req = {
        type: 'agent_request',
        request_id: 'int-test-1',
        config: {
          prompt,
          agentName: 'scope-table-details-analyzer',
          apiKey,
          cwd,
        },
      };

      child.stdin.write(JSON.stringify(req) + '\n');

      // Wait for request_complete
      await new Promise<void>((res, rej) => {
        const t = setTimeout(() => rej(new Error('request_complete timeout')), 120_000);
        const iv = setInterval(() => {
          if (messages.some((m) => m['type'] === 'request_complete')) {
            clearInterval(iv); clearTimeout(t); res();
          }
        }, 100);
      });

      child.stdin.write('{"type":"shutdown"}\n');
      await new Promise<void>((res) => child.on('exit', () => res()));

      // ── Collect assistant messages ────────────────────────────────────────────
      const assistantMessages = messages.filter(
        (m) => m['type'] === 'assistant' &&
          typeof (m['message'] as Record<string, unknown>)?.['model'] === 'string',
      );

      // ── Tool call analysis ────────────────────────────────────────────────────
      const toolNamesCalled: string[] = [];
      for (const m of messages) {
        if (m['type'] !== 'assistant') continue;
        const content = (m['message'] as Record<string, unknown>)?.['content'];
        if (!Array.isArray(content)) continue;
        for (const block of content as Record<string, unknown>[]) {
          if (block['type'] === 'tool_use' && typeof block['name'] === 'string') {
            toolNamesCalled.push(block['name'] as string);
          }
        }
      }

      // V1 SDK emits the final output as type='result' with a 'result' field
      const agentResponses = messages.filter(
        (m) => m['type'] === 'result' && typeof m['result'] === 'string' && (m['result'] as string).length > 0,
      );

      // ── Summary (stderr for visibility) ──────────────────────────────────────
      const modelsUsed = [...new Set(
        assistantMessages.map(m => (m['message'] as Record<string, unknown>)?.['model'] as string)
      )];
      process.stderr.write(`\n=== MODEL USED: ${modelsUsed.join(', ') || 'unknown'} ===\n`);
      process.stderr.write(`\n=== TOOL CALLS (${toolNamesCalled.length}) ===\n`);
      for (const t of toolNamesCalled) process.stderr.write(`  ${t}\n`);
      process.stderr.write('\n=== AGENT TEXT RESPONSES ===\n');
      for (const r of agentResponses) process.stderr.write((r['result'] as string) + '\n');
      process.stderr.write('============================\n');

      // ── Assertions ────────────────────────────────────────────────────────────

      // Model: sidecar should resolve model from front-matter and use haiku, not sonnet
      expect(modelsUsed).toHaveLength(1);
      expect(modelsUsed[0]).toMatch(/claude-haiku/);

      // Tool restriction: only Bash should be called — no MCP tools (Slack, Gmail, Linear, etc.)
      const allowedTools = new Set(['Bash', 'Read', 'Write', 'Edit', 'Glob', 'Grep', 'LS']);
      const mcpToolsCalled = toolNamesCalled.filter((name) => !allowedTools.has(name));
      expect(mcpToolsCalled).toHaveLength(0);

      // Result: agent must return a valid JSON object
      expect(agentResponses.length).toBeGreaterThanOrEqual(1);
      const raw = agentResponses[agentResponses.length - 1]['result'] as string;
      // Strip optional markdown code fence (agent may wrap output in ```json ... ```)
      const resultText = raw.replace(/^```(?:json)?\s*\n?/, '').replace(/\n?```\s*$/, '').trim();
      const parsed = JSON.parse(resultText);
      expect(parsed).toHaveProperty('table_type');
      expect(parsed).toHaveProperty('load_strategy');
      expect(parsed).toHaveProperty('grain_columns');

    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  }, 150_000);
});
