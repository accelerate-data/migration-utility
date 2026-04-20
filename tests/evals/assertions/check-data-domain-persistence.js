const fs = require('fs');
const path = require('path');
const { normalizeTerms, resolveProjectPath } = require('./schema-helpers');

const REQUIRED_FIELDS = [
  'schema_version',
  'domain',
  'slug',
  'status',
  'description',
  'confidence',
  'objects',
  'setup_source_candidates',
  'dependencies',
  'ambiguities',
  'rationale',
];
const VOLATILE_FIELDS = new Set([
  'created_at',
  'createdAt',
  'generated_at',
  'generatedAt',
  'timestamp',
  'updated_at',
  'updatedAt',
]);

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function listJsonFiles(root) {
  if (!fs.existsSync(root)) {
    return [];
  }

  return fs.readdirSync(root, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith('.json'))
    .map((entry) => path.join(root, entry.name))
    .sort();
}

function relativeJsonBasenames(files) {
  return files.map((filePath) => path.basename(filePath)).sort();
}

function canonicalJson(value) {
  return JSON.stringify(value, null, 2) + '\n';
}

function hasForbiddenObjectBuckets(domain) {
  const objects = domain.objects || {};
  return Object.keys(objects).some((key) => !['tables', 'views'].includes(key));
}

function hasVolatileField(value) {
  if (Array.isArray(value)) {
    return value.some(hasVolatileField);
  }
  if (!value || typeof value !== 'object') {
    return false;
  }

  return Object.entries(value).some(([key, child]) => {
    return VOLATILE_FIELDS.has(key) || hasVolatileField(child);
  });
}

function isSortedArray(value) {
  if (!Array.isArray(value)) {
    return true;
  }
  const sorted = [...value].sort((left, right) => {
    return String(left).localeCompare(String(right));
  });
  return value.every((entry, index) => entry === sorted[index]);
}

function findUnsortedArray(value, pathParts = []) {
  if (Array.isArray(value)) {
    if (pathParts.includes('ambiguities') || pathParts.includes('rationale')) {
      return null;
    }
    if (value.some((entry) => entry && typeof entry === 'object')) {
      return null;
    }
    return isSortedArray(value) ? null : pathParts.join('.');
  }
  if (!value || typeof value !== 'object') {
    return null;
  }

  for (const [key, child] of Object.entries(value)) {
    const result = findUnsortedArray(child, [...pathParts, key]);
    if (result) {
      return result;
    }
  }
  return null;
}

function normalizeObject(value) {
  return String(value || '').trim().toLowerCase();
}

function objectSet(values) {
  return new Set((Array.isArray(values) ? values : []).map(normalizeObject));
}

function domainObjectValues(domain) {
  return Object.values(domain.objects || {})
    .filter(Array.isArray)
    .flat()
    .map(normalizeObject);
}

function duplicatePrimaryOwnership(domains) {
  const owners = new Map();
  for (const domain of domains) {
    for (const objectName of domainObjectValues(domain)) {
      const previous = owners.get(objectName);
      if (previous) {
        return { objectName, previous, next: domain.slug };
      }
      owners.set(objectName, domain.slug);
    }
  }
  return null;
}

function parseExpectedAssignments(value) {
  return String(value || '')
    .split(';')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const [domainSlug, rest] = entry.split(':');
      const [bucket, objectName] = rest.split('=');
      return {
        domainSlug: normalizeObject(domainSlug),
        bucket: normalizeObject(bucket),
        objectName: normalizeObject(objectName),
      };
    });
}

function domainBySlug(domains, slug) {
  return domains.find((domain) => normalizeObject(domain.slug) === normalizeObject(slug));
}

module.exports = (_output, context) => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const fixtureRoot = path.resolve(
    repoRoot,
    context.vars.canonical_fixture_path || context.vars.fixture_path || '',
  );
  const catalogRoot = path.join(runRoot, 'warehouse-catalog', 'data-domains');
  const files = listJsonFiles(catalogRoot);

  if (files.length === 0) {
    return fail('No data-domain files were written');
  }

  if (fs.existsSync(path.join(runRoot, 'catalog'))) {
    return fail('Unexpected catalog/ directory was written');
  }

  const domains = files.map(readJson);
  for (const relativePath of normalizeTerms(context.vars.forbidden_catalog_paths)) {
    const forbiddenPath = path.join(runRoot, relativePath);
    if (fs.existsSync(forbiddenPath)) {
      return fail(`Forbidden catalog path was written: ${relativePath}`);
    }
  }

  const expectedFiles = normalizeTerms(context.vars.expected_domain_files);
  for (const slug of expectedFiles) {
    if (!files.some((filePath) => path.basename(filePath) === `${slug}.json`)) {
      return fail(`Expected data-domain file '${slug}.json' was not written`);
    }
  }
  if (expectedFiles.length > 0) {
    const expectedBasenames = expectedFiles.map((slug) => `${slug}.json`).sort();
    const actualBasenames = relativeJsonBasenames(files);
    const unexpected = actualBasenames.filter((fileName) => !expectedBasenames.includes(fileName));
    if (unexpected.length > 0) {
      return fail(`Unexpected data-domain files: ${unexpected.join(', ')}`);
    }
  }

  for (const domain of domains) {
    const fieldOrderError = stableTopLevelOrderError(domain);
    if (fieldOrderError) {
      return fail(fieldOrderError);
    }

    for (const field of REQUIRED_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(domain, field)) {
        return fail(`Domain '${domain.slug || domain.domain}' is missing required field '${field}'`);
      }
    }

    if (hasForbiddenObjectBuckets(domain)) {
      return fail(`Domain '${domain.slug}' contains non-table-view object buckets`);
    }

    if (!Array.isArray(domain.objects?.tables)) {
      return fail(`Domain '${domain.slug}' is missing required objects.tables bucket`);
    }
    if (!Array.isArray(domain.objects?.views)) {
      return fail(`Domain '${domain.slug}' is missing required objects.views bucket`);
    }

    if (hasVolatileField(domain)) {
      return fail(`Domain '${domain.slug}' contains volatile timestamp fields`);
    }

    const unsortedArrayPath = findUnsortedArray(domain);
    if (unsortedArrayPath) {
      return fail(`Domain '${domain.slug}' contains unsorted array '${unsortedArrayPath}'`);
    }

    const forbiddenObjects = normalizeTerms(context.vars.forbidden_domain_objects);
    const objectValues = new Set(domainObjectValues(domain));
    for (const objectName of forbiddenObjects) {
      if (objectValues.has(objectName)) {
        return fail(`Domain '${domain.slug}' contains forbidden domain object '${objectName}'`);
      }
    }
  }

  const duplicate = duplicatePrimaryOwnership(domains);
  if (duplicate) {
    return fail(
      `Object '${duplicate.objectName}' has multiple primary domains: '${duplicate.previous}' and '${duplicate.next}'`,
    );
  }

  for (const slug of normalizeTerms(context.vars.stable_domain_files_from_fixture)) {
    const fixtureFile = path.join(fixtureRoot, 'warehouse-catalog', 'data-domains', `${slug}.json`);
    const runFile = path.join(catalogRoot, `${slug}.json`);
    if (!fs.existsSync(fixtureFile)) {
      return fail(`Canonical fixture data-domain file '${slug}.json' was not found`);
    }
    if (!fs.existsSync(runFile)) {
      return fail(`Run data-domain file '${slug}.json' was not written`);
    }

    const fixtureDomain = readJson(fixtureFile);
    const runDomain = readJson(runFile);
    if (canonicalJson(runDomain) !== canonicalJson(fixtureDomain)) {
      return fail(`Data-domain file '${slug}.json' changed from canonical fixture`);
    }
  }

  for (const assignment of parseExpectedAssignments(context.vars.expected_domain_objects)) {
    const domain = domainBySlug(domains, assignment.domainSlug);
    if (!domain) {
      return fail(`Expected domain '${assignment.domainSlug}' was not written`);
    }

    const objects = objectSet(domain.objects?.[assignment.bucket]);
    if (!objects.has(assignment.objectName)) {
      return fail(
        `Expected '${assignment.objectName}' in '${assignment.domainSlug}.${assignment.bucket}'`,
      );
    }

    for (const otherDomain of domains.filter((candidate) => candidate !== domain)) {
      const otherObjects = objectSet(otherDomain.objects?.[assignment.bucket]);
      if (otherObjects.has(assignment.objectName)) {
        return fail(
          `Object '${assignment.objectName}' appeared in both '${assignment.domainSlug}' and '${otherDomain.slug}'`,
        );
      }
    }
  }

  const expectedUpstream = parseExpectedAssignments(context.vars.expected_upstream_domains);
  for (const dependency of expectedUpstream) {
    const domain = domainBySlug(domains, dependency.domainSlug);
    if (!domain) {
      return fail(`Expected dependency domain '${dependency.domainSlug}' was not written`);
    }

    const upstream = objectSet(domain.dependencies?.upstream_domains);
    if (!upstream.has(dependency.objectName)) {
      return fail(
        `Expected upstream domain '${dependency.objectName}' for '${dependency.domainSlug}'`,
      );
    }
  }

  return {
    pass: true,
    score: 1,
    reason: `Validated persisted data-domain files: ${files.map((filePath) => path.basename(filePath)).join(', ')}`,
  };
};

function stableTopLevelOrderError(domain) {
  const keys = Object.keys(domain);
  for (const [index, expectedKey] of REQUIRED_FIELDS.entries()) {
    if (keys[index] !== expectedKey) {
      return `Domain '${domain.slug || domain.domain}' has unstable field order: expected '${expectedKey}' before '${keys[index]}'`;
    }
  }
  return null;
}
