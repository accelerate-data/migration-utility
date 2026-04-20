const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkTableScoping = require('./check-table-scoping');

const repoRoot = path.resolve(__dirname, '..', '..', '..');

test('check-table-scoping accepts status from latest run artifact', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'table-scoping-'));
  const runPath = path.relative(repoRoot, tmp);
  const catalogDir = path.join(tmp, 'catalog', 'tables');
  const runsDir = path.join(tmp, '.migration-runs');
  fs.mkdirSync(catalogDir, { recursive: true });
  fs.mkdirSync(runsDir, { recursive: true });

  fs.writeFileSync(
    path.join(catalogDir, 'sh.channel_sales_summary.json'),
    JSON.stringify({
      schema: 'sh',
      name: 'channel_sales_summary',
      scoping: {
        selected_writer: 'sh.SUMMARIZE_CHANNEL_SALES',
      },
    }),
  );
  fs.writeFileSync(
    path.join(runsDir, 'sh.channel_sales_summary.1234.json'),
    JSON.stringify({
      item_id: 'sh.CHANNEL_SALES_SUMMARY',
      status: 'resolved',
    }),
  );

  try {
    const result = checkTableScoping('', {
      vars: {
        run_path: runPath,
        target_table: 'sh.channel_sales_summary',
        expected_status: 'resolved',
        expected_writer: 'summarize_channel_sales',
      },
    });

    assert.equal(result.pass, true, result.reason);
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});
