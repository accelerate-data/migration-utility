const fs = require('fs');
const path = require('path');
const { resolveProjectPath } = require('./schema-helpers');

function fail(reason) {
  return { pass: false, score: 0, reason };
}

function readIfExists(filePath) {
  if (!fs.existsSync(filePath)) {
    return '';
  }
  return fs.readFileSync(filePath, 'utf8');
}

function parseStages(markdown) {
  const text = String(markdown || '');
  const headingPattern = /^## Stage (\d{3}):\s+(.+?)\s*$/gm;
  const stages = [];
  let match;

  while ((match = headingPattern.exec(text)) !== null) {
    const stageId = match[1];
    const stageName = match[2].trim();
    const bodyStart = headingPattern.lastIndex;
    const nextMatch = /^## Stage \d{3}:\s+.+?\s*$/gm;
    nextMatch.lastIndex = bodyStart;
    const next = nextMatch.exec(text);
    const body = text.slice(bodyStart, next ? next.index : text.length);
    const statusMatch = body.match(/^-+\s*Status:\s*(.+?)\s*$/im);
    stages.push({
      id: stageId,
      name: stageName,
      status: statusMatch ? normalizeStatus(statusMatch[1]) : '',
      body,
    });
  }

  return stages;
}

function normalizeStatus(value) {
  return String(value || '')
    .trim()
    .replace(/^`+|`+$/g, '')
    .toLowerCase();
}

function firstIncompleteStage(stages) {
  return stages.find((stage) => !['complete', 'skipped', 'superseded'].includes(stage.status));
}

function lower(text) {
  return String(text || '').toLowerCase();
}

function expectedTerms(value) {
  return String(value || '')
    .split(',')
    .map((term) => term.trim())
    .filter(Boolean);
}

module.exports = (output, context) => {
  const repoRoot = path.resolve(__dirname, '..', '..', '..');
  const runRoot = path.resolve(repoRoot, resolveProjectPath(context));
  const planFile = context.vars.plan_file;
  if (!planFile) {
    return fail('plan_file var is required');
  }

  const planPath = path.join(runRoot, planFile);
  const outputText = String(output || '');
  const planText = readIfExists(planPath);
  const evidence = `${outputText}\n${planText}`.toLowerCase();
  const blockerTerms = expectedTerms(context.vars.expected_blocker_terms);

  if (blockerTerms.length > 0) {
    for (const term of blockerTerms) {
      if (!evidence.includes(term.toLowerCase())) {
        return fail(`Missing expected blocker term '${term}'`);
      }
    }
    if (/\bstart[- ]stage\b|--start-stage/i.test(evidence)) {
      return fail('Output or plan evidence mentioned a start-stage argument');
    }
    return {
      pass: true,
      score: 1,
      reason: 'Migrate mart resume blocker contract validated',
    };
  }

  const parseSource = planText || outputText;
  const stages = parseStages(parseSource);

  if (stages.length === 0) {
    return fail(`No stage sections found in plan evidence at ${planPath}`);
  }

  const expectedStage = context.vars.expected_resume_stage
    ? String(context.vars.expected_resume_stage).trim()
    : null;
  const expectedStageName = context.vars.expected_resume_stage_name
    ? String(context.vars.expected_resume_stage_name).trim().toLowerCase()
    : null;
  const resumeStage = firstIncompleteStage(stages);
  if (!resumeStage) {
    return fail('Plan does not contain any incomplete stages to resume');
  }

  if (expectedStage && resumeStage.id !== expectedStage) {
    return fail(`Expected first incomplete stage ${expectedStage}, found ${resumeStage.id}`);
  }

  if (expectedStageName && !lower(resumeStage.name).includes(expectedStageName)) {
    return fail(`Expected first incomplete stage name '${expectedStageName}', found '${resumeStage.name}'`);
  }

  const outputLower = lower(outputText);
  const stagePattern = new RegExp(`stage\\s*(?:[:#-]\\s*)?\\**${resumeStage.id}\\**`, 'i');
  const selectedStageId = stagePattern.test(outputText) || outputLower.includes(`stage ${resumeStage.id}`);
  const selectedStageName = outputLower.includes(lower(resumeStage.name)) ||
    (expectedStageName && outputLower.includes(expectedStageName));
  if (!selectedStageId || !selectedStageName) {
    return fail(`Output did not identify the first incomplete stage ${resumeStage.id} (${resumeStage.name})`);
  }

  if (/\bstart[- ]stage\b|--start-stage/i.test(`${outputText}\n${planText}`)) {
    return fail('Output or plan evidence mentioned a start-stage argument');
  }

  const earlierIncomplete = stages.slice(0, stages.indexOf(resumeStage)).filter((stage) =>
    !['complete', 'skipped', 'superseded'].includes(stage.status),
  );
  if (earlierIncomplete.length > 0) {
    return fail(`Stage ${resumeStage.id} was not the first incomplete stage in the plan`);
  }

  return {
    pass: true,
    score: 1,
    reason: `Resumed first incomplete stage ${resumeStage.id} (${resumeStage.name})`,
  };
};
