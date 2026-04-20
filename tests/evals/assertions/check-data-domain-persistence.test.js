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

test('check-data-domain-persistence rejects non-table-view object buckets', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: {
        tables: ['silver.opportunities'],
        views: [],
        supporting_objects: ['silver.load_opportunities'],
      },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /non-table-view object buckets/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects routine objects in allowed buckets', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.load_opportunities', 'silver.opportunities'], views: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
        forbidden_domain_objects: 'silver.load_opportunities,gold.opportunity_value',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /forbidden domain object 'silver\.load_opportunities'/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects unstable top-level field order', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    const filePath = path.join(runRoot, 'warehouse-catalog', 'data-domains', 'sales.json');
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(
      filePath,
      JSON.stringify({
        domain: 'sales',
        schema_version: 1,
        slug: 'sales',
        status: 'candidate',
        description: 'sales domain',
        confidence: 'medium',
        objects: { tables: [], views: [] },
        setup_source_candidates: { schemas: [], tables: [] },
        dependencies: { upstream_domains: [], downstream_domains: [] },
        ambiguities: [],
        rationale: [],
      }, null, 2) + '\n',
      'utf8',
    );

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /unstable field order/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects volatile timestamp fields', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      generated_at: '2026-04-20T00:00:00Z',
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /volatile timestamp fields/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects unsorted arrays', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.z_table', 'silver.a_table'], views: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /unsorted array 'objects\.tables'/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects unexpected extra domain files', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales');
    writeDomain(runRoot, 'finance');

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /unexpected data-domain files: finance\.json/i);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence rejects duplicate primary object ownership across domains', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.opportunities'], views: [] },
    });
    writeDomain(runRoot, 'operations', {
      objects: { tables: ['silver.opportunities'], views: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales,operations',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /multiple primary domains/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence requires explicit tables and views object buckets', () => {
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.opportunities'] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        run_path: runRoot,
        expected_domain_files: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /missing required objects\.views bucket/);
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});

test('check-data-domain-persistence compares stable files to the canonical fixture', () => {
  const fixtureRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-fixture-'));
  const runRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'domain-run-'));
  try {
    writeDomain(fixtureRoot, 'sales', {
      objects: { tables: ['silver.opportunities'], views: [] },
    });
    writeDomain(runRoot, 'sales', {
      objects: { tables: ['silver.opportunities', 'silver.sales_forecast'], views: [] },
    });

    const result = checkDataDomainPersistence('', {
      vars: {
        fixture_path: fixtureRoot,
        run_path: runRoot,
        expected_domain_files: 'sales',
        stable_domain_files_from_fixture: 'sales',
      },
    });

    assert.equal(result.pass, false);
    assert.match(result.reason, /changed from canonical fixture/);
  } finally {
    fs.rmSync(fixtureRoot, { recursive: true, force: true });
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
});
