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

function hasForbiddenObjectBuckets(domain) {
  const objects = domain.objects || {};
  return ['procedures', 'functions'].some((key) => Object.prototype.hasOwnProperty.call(objects, key));
}

function normalizeObject(value) {
  return String(value || '').trim().toLowerCase();
}

function objectSet(values) {
  return new Set((Array.isArray(values) ? values : []).map(normalizeObject));
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
  const catalogRoot = path.join(runRoot, 'warehouse-catalog', 'data-domains');
  const files = listJsonFiles(catalogRoot);

  if (files.length === 0) {
    return fail('No data-domain files were written');
  }

  if (fs.existsSync(path.join(runRoot, 'catalog'))) {
    return fail('Unexpected catalog/ directory was written');
  }

  const domains = files.map(readJson);
  const expectedFiles = normalizeTerms(context.vars.expected_domain_files);
  for (const slug of expectedFiles) {
    if (!files.some((filePath) => path.basename(filePath) === `${slug}.json`)) {
      return fail(`Expected data-domain file '${slug}.json' was not written`);
    }
  }

  for (const domain of domains) {
    for (const field of REQUIRED_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(domain, field)) {
        return fail(`Domain '${domain.slug || domain.domain}' is missing required field '${field}'`);
      }
    }

    if (hasForbiddenObjectBuckets(domain)) {
      return fail(`Domain '${domain.slug}' contains procedure/function object buckets`);
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
