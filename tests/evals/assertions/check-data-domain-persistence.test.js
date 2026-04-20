const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const checkDataDomainPersistence = require('./check-data-domain-persistence');

function writeDomain(root, slug, patch = {}) {
  const value = {
    schema_version: 1,
    domain: slug,
    slug,
    status: 'candidate',
    description: `${slug} domain`,
    confidence: 'medium',
    objects: { tables: [], views: [] },
    setup_source_candidates: { schemas: [], tables: [] },
    dependencies: { upstream_domains: [], downstream_domains: [] },
    ambiguities: [],
    rationale: [],
    ...patch,
  };
  const filePath = path.join(root, 'warehouse-catalog', 'data-domains', `${slug}.json`);
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2) + '\n', 'utf8');
}

test('check-data-domain-persistence validates expected object ownership', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.opportunities'], views: [] },
    });
    writeDomain(runRoot, 'operations', {
      objects: { tables: [], views: ['gold.sold_opportunities'] },
      dependencies: { upstream_domains: ['sales'], downstream_domains: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales,operations',
        expected_domain_objects: 'sales:tables=silver.opportunities;operations:views=gold.sold_opportunities',
        expected_upstream_domains: 'operations:upstream_domains=sales',
      },
    });

    assert.equal(result.pass, true);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects procedure buckets', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.opportunities'], views: [], procedures: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /procedure\/function/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});
