// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const fs = require('fs');
const path = require('path');

function skip(core, message) {
  core.info(message);
  core.setOutput('should_post', 'false');
}

// Trusted chain constants for the comment validator. The comment workflow
// follows the `Query Regression Controller` run; the controller followed the
// unprivileged `Query Regression` build run and regenerated the comment
// metadata from its validated values.
const SOURCE_WORKFLOW_NAME = 'Query Regression';
const CONTROLLER_WORKFLOW_NAME = 'Query Regression Controller';
// Exact workflow file paths. REST/payload `path` appends the triggering ref
// (`.github/workflows/x.yml@refs/...`); canonicalRunPath() strips that first
// `@ref` suffix before comparing. Display-name checks (run.name) are retained
// as defense in depth.
const SOURCE_WORKFLOW_PATH = '.github/workflows/query-regression.yml';
const CONTROLLER_WORKFLOW_PATH = '.github/workflows/query-regression-controller.yml';
const FULL_SHA_RE = /^[0-9a-f]{40}$/i;

function isPositiveInt(value) {
  return Number.isInteger(value) && value > 0;
}

function isFullSha(value) {
  return typeof value === 'string' && FULL_SHA_RE.test(value);
}

// GitHub's REST/workflow_run payload `path` appends the triggering ref
// (`.github/workflows/x.yml@refs/heads/main`). Canonicalize to the workflow
// file path alone by stripping the first `@ref` suffix; unqualified paths
// (no `@`) are returned unchanged.
function canonicalRunPath(runPath) {
  if (typeof runPath !== 'string') {
    return runPath;
  }
  const atIndex = runPath.indexOf('@');
  return atIndex === -1 ? runPath : runPath.slice(0, atIndex);
}

function findReports(dir) {
  const reports = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      reports.push(...findReports(full));
    } else if (entry.isFile() && entry.name === 'query-regression-report.json') {
      reports.push(full);
    }
  }
  return reports.sort();
}

function text(value) {
  if (value === null || value === undefined || value === '') return 'N/A';
  const result = String(value)
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '&#96;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/\(/g, '\\(')
    .replace(/\)/g, '\\)')
    .replace(/!/g, '\\!')
    .replace(/@/g, '@\u200b')
    .replace(/\|/g, '\\|')
    .replace(/\r\n|\r|\n/g, ' ');
  return result;
}

function statusEmoji(status) {
  return { ok: '✅', measured: '✅', failed: '❌', planned: '📝', 'fixture-ready': '🧪' }[status] || '⚠️';
}

function finiteNumber(value) {
  if (typeof value !== 'number' && typeof value !== 'string') {
    return null;
  }
  if (typeof value === 'string' && value.trim() === '') {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function fmtMs(value) {
  const number = finiteNumber(value);
  return number === null ? 'N/A' : number.toFixed(2);
}

function measurementsByName(target) {
  const result = new Map();
  const measurements = Array.isArray(target?.measurements) ? target.measurements : [];
  measurements.forEach((measurement, index) => {
    result.set(String(measurement?.name || `query-${index}`), measurement || {});
  });
  return result;
}

function regression(base, candidate) {
  const b = finiteNumber(base);
  const c = finiteNumber(candidate);
  if (b === null || c === null || b === 0) return 'N/A';
  return `${(((c - b) / b) * 100).toFixed(1)}%`;
}

function thresholdStatus(thresholds, query) {
  const hits = (Array.isArray(thresholds) ? thresholds : [])
    .filter(item => query === undefined || (hasScopedQuery(item) && String(item.query) === query))
    .map(formatThreshold);
  return hits.length > 0 ? hits.join('; ') : 'N/A';
}

function hasScopedQuery(threshold) {
  return threshold?.query !== null
    && threshold?.query !== undefined
    && String(threshold.query) !== '';
}

function hasValue(value) {
  return value !== null && value !== undefined && String(value) !== '';
}

function formatThreshold(threshold) {
  const scope = [];
  if (hasValue(threshold?.target)) scope.push(`target=${threshold.target}`);
  if (hasValue(threshold?.encoding)) scope.push(`encoding=${threshold.encoding}`);
  const name = threshold?.threshold || 'threshold';
  const status = threshold?.status || 'unknown';
  const reason = hasValue(threshold?.reason) ? ` (reason: ${threshold.reason})` : '';
  return `${name}${scope.length > 0 ? ` [${scope.join(', ')}]` : ''}: ${status}${reason}`;
}

function classifyThresholds(thresholds, measurementNames) {
  const unscoped = [];
  const unmatched = new Map();
  for (const threshold of Array.isArray(thresholds) ? thresholds : []) {
    if (!hasScopedQuery(threshold)) {
      unscoped.push(threshold);
      continue;
    }
    const query = String(threshold.query);
    if (!measurementNames.has(query)) {
      const entries = unmatched.get(query) || [];
      entries.push(threshold);
      unmatched.set(query, entries);
    }
  }
  return { unscoped, unmatched };
}

function syntheticThresholdStatus(thresholds, measurementNames) {
  const { unscoped, unmatched } = classifyThresholds(thresholds, measurementNames);
  const parts = [];
  if (unscoped.length > 0) {
    parts.push(`case/storage threshold: ${thresholdStatus(unscoped)}`);
  }
  for (const query of Array.from(unmatched.keys()).sort()) {
    parts.push(`unmatched query ${query}: ${thresholdStatus(unmatched.get(query))}`);
  }
  return parts.length > 0 ? parts.join('; ') : 'N/A';
}

// Case-level (query-unscoped) thresholds formatted for the collapsible details
// block below the summary table: individual items, `; `-separated, without the
// `case/storage threshold:` label (the section heading already says that).
function syntheticThresholdDetail(thresholds, measurementNames) {
  const { unscoped, unmatched } = classifyThresholds(thresholds, measurementNames);
  const items = [];
  if (unscoped.length > 0) {
    items.push(thresholdStatus(unscoped));
  }
  for (const query of Array.from(unmatched.keys()).sort()) {
    items.push(`unmatched query ${query}: ${thresholdStatus(unmatched.get(query))}`);
  }
  return items.length > 0 ? items.join('; ') : 'N/A';
}

function joinDetails(...details) {
  const present = details.filter(detail => detail && detail !== 'N/A');
  return present.length > 0 ? present.join('; ') : 'N/A';
}

function missingMeasurementDetails(base, candidate) {
  const details = [];
  if (finiteNumber(base?.latency_ms_median) === null) details.push('base measurement missing');
  if (finiteNumber(candidate?.latency_ms_median) === null) details.push('candidate measurement missing');
  return details;
}

function collectReportRows(report, reportPath) {
  const fallbackName = typeof reportPath === 'string'
    ? path.basename(path.dirname(reportPath)) || 'unknown'
    : 'unknown';
  if (report === null || Array.isArray(report) || typeof report !== 'object') {
    return [{
      caseName: fallbackName,
      query: 'N/A',
      status: 'missing',
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: 'invalid report object',
    }];
  }
  const caseInfo = report.case || {};
  const name = caseInfo.name || fallbackName;
  const status = report.status || 'missing';
  const thresholds = Array.isArray(report.thresholds) ? report.thresholds : [];
  if (report.error) {
    return [{
      caseName: name,
      query: 'N/A',
      status,
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: joinDetails(`error: ${report.error}`, syntheticThresholdStatus(thresholds, new Set())),
    }];
  }

  const targets = Array.isArray(report.targets) ? report.targets : [];
  if (targets.length < 2) {
    return [{
      caseName: name,
      query: 'N/A',
      status,
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: joinDetails('base/candidate measurements missing', syntheticThresholdStatus(thresholds, new Set())),
    }];
  }

  const base = measurementsByName(targets[0]);
  const candidate = measurementsByName(targets[1]);
  const names = Array.from(new Set([...base.keys(), ...candidate.keys()])).sort();
  if (names.length === 0) {
    return [{
      caseName: name,
      query: 'N/A',
      status,
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: joinDetails('no query measurements found', syntheticThresholdStatus(thresholds, new Set())),
    }];
  }

  const measurementNames = new Set(names);
  const rows = names.map(query => {
    const b = base.get(query) || {};
    const c = candidate.get(query) || {};
    return {
      caseName: name,
      query,
      status,
      baseMedian: fmtMs(b.latency_ms_median),
      candidateMedian: fmtMs(c.latency_ms_median),
      regression: regression(b.latency_ms_median, c.latency_ms_median),
      threshold: joinDetails(
        ...missingMeasurementDetails(b, c),
        thresholdStatus(thresholds, query)
      ),
    };
  });
  const syntheticThresholds = syntheticThresholdDetail(thresholds, measurementNames);
  if (syntheticThresholds !== 'N/A') {
    rows.push({
      caseName: name,
      query: 'N/A',
      status,
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: syntheticThresholds,
      kind: 'case-thresholds',
    });
  }
  return rows;
}

function renderThresholdDetails(rows) {
  const entries = rows.filter(row => row.kind === 'case-thresholds');
  if (entries.length === 0) {
    return '';
  }
  // The <details>/<summary> tags are emitted literally; only the entry content
  // goes through text() (so `[ ] ( ) |` etc. stay escaped while the tags render).
  const lines = [
    '<details><summary>Case / storage thresholds</summary>',
    '',
  ];
  for (const row of entries) {
    lines.push(`- ${text(row.caseName)}: ${text(row.threshold)}`);
  }
  lines.push('</details>');
  return lines.join('\n');
}

function renderSummaryTable(rows) {
  const lines = [
    '| Case | Query | Case status | Base median ms | Candidate median ms | Regression | Threshold |',
    '| --- | --- | --- | ---: | ---: | ---: | --- |',
  ];
  for (const row of rows) {
    if (row.kind === 'case-thresholds') {
      continue;
    }
    lines.push(
      `| ${text(row.caseName)} | ${text(row.query)} | ${statusEmoji(row.status)} ${text(row.status)} | ${text(row.baseMedian)} | ${text(row.candidateMedian)} | ${text(row.regression)} | ${text(row.threshold)} |`
    );
  }
  const details = renderThresholdDetails(rows);
  const table = lines.join('\n');
  return details === '' ? table : `${table}\n\n${details}`;
}

module.exports = async function validateQueryRegressionComment({ github, context, core }) {
  const artifactDir = 'query-regression-comment';
  const metadataPath = path.join(artifactDir, 'query-regression-pr.json');
  const summaryPath = path.join(artifactDir, 'query-regression-summary.md');

  if (!fs.existsSync(metadataPath)) {
    return skip(core, 'Missing query-regression-pr.json; skipping sticky comment.');
  }

  let metadata;
  try {
    metadata = JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
  } catch (error) {
    core.warning(`Invalid PR metadata JSON: ${error.message}`);
    return skip(core, 'Invalid PR metadata JSON; skipping sticky comment.');
  }

  // The immediate run this workflow follows is the trusted controller run.
  // The comment artifact was downloaded from THIS run by exact artifact id
  // (see query-regression-comment.yml), so pinning the regenerated metadata
  // to this run's id/attempt rejects replayed artifacts from any other run.
  const controllerRunId = Number(process.env.WORKFLOW_RUN_ID);
  const controllerRunAttempt = Number(process.env.WORKFLOW_RUN_ATTEMPT);
  if (!isPositiveInt(controllerRunId) || !isPositiveInt(controllerRunAttempt)) {
    return skip(core, 'WORKFLOW_RUN_ID/WORKFLOW_RUN_ATTEMPT must be positive integers; skipping.');
  }
  if (metadata.run_id !== controllerRunId || metadata.run_attempt !== controllerRunAttempt) {
    return skip(core, 'Artifact metadata does not match this controller workflow_run; skipping.');
  }

  if (metadata.base_repo !== `${context.repo.owner}/${context.repo.repo}`) {
    return skip(core, `PR targets ${metadata.base_repo}, not this repository; skipping.`);
  }

  const prNumber = Number(metadata.pr_number);
  if (!isPositiveInt(prNumber)) {
    return skip(core, 'Invalid PR number in metadata; skipping.');
  }
  if (!isFullSha(metadata.head_sha)) {
    return skip(core, 'Metadata head_sha is not a full SHA; skipping.');
  }
  if (typeof metadata.head_repo !== 'string' || metadata.head_repo === '') {
    return skip(core, 'Metadata head_repo is missing; skipping.');
  }
  const sourceRunId = Number(metadata.source_run_id);
  const sourceRunAttempt = Number(metadata.source_run_attempt);
  if (!isPositiveInt(sourceRunId) || !isPositiveInt(sourceRunAttempt)) {
    return skip(core, 'Metadata source_run_id/source_run_attempt are missing or invalid; skipping.');
  }

  // 1. Independently validate the immediate controller run. The workflow_run
  // payload is the trust anchor (GitHub-signed); the API re-check makes the
  // validation independent of the payload for the fields that matter.
  const run = context.payload.workflow_run;
  if (!run) {
    return skip(core, 'Workflow run payload is missing; skipping.');
  }
  if (run.event !== 'workflow_run') {
    return skip(core, `Controller run event is ${run.event}, not workflow_run; skipping.`);
  }
  if (run.name !== CONTROLLER_WORKFLOW_NAME) {
    return skip(core, `Immediate run belongs to ${JSON.stringify(run.name)}, not ${CONTROLLER_WORKFLOW_NAME}; skipping.`);
  }
  if (canonicalRunPath(run.path) !== CONTROLLER_WORKFLOW_PATH) {
    return skip(core, `Immediate run is at ${JSON.stringify(run.path)}, not ${CONTROLLER_WORKFLOW_PATH}; skipping.`);
  }
  if (run.conclusion === 'cancelled' || run.conclusion === 'skipped') {
    return skip(core, `Controller run conclusion is ${run.conclusion}; skipping.`);
  }
  const runHeadRepo = run.head_repository?.full_name;
  if (!runHeadRepo) {
    return skip(core, 'Controller run head repository is missing; skipping.');
  }
  if (runHeadRepo !== metadata.base_repo) {
    return skip(core, 'Controller run head repository differs from the base repository; skipping.');
  }

  let apiRun;
  try {
    ({ data: apiRun } = await github.rest.actions.getWorkflowRun({
      owner: context.repo.owner,
      repo: context.repo.repo,
      run_id: controllerRunId,
    }));
  } catch (error) {
    core.warning(`Could not verify controller run #${controllerRunId}: ${error.message}`);
    return skip(core, 'Could not verify the controller run via the API; skipping.');
  }
  if (apiRun.id !== controllerRunId || apiRun.name !== CONTROLLER_WORKFLOW_NAME || apiRun.event !== 'workflow_run') {
    return skip(core, 'Controller run does not match the expected trusted workflow; skipping.');
  }
  if (canonicalRunPath(apiRun.path) !== CONTROLLER_WORKFLOW_PATH) {
    return skip(core, `Controller run is at ${JSON.stringify(apiRun.path)}, not ${CONTROLLER_WORKFLOW_PATH}; skipping.`);
  }
  if (apiRun.conclusion === 'cancelled' || apiRun.conclusion === 'skipped') {
    return skip(core, `Controller run conclusion is ${apiRun.conclusion}; skipping.`);
  }
  if (apiRun.head_repository?.full_name !== metadata.base_repo) {
    return skip(core, 'Controller run head repository differs from the base repository; skipping.');
  }

  // 2. Resolve and validate the ORIGINAL source build run. The controller
  // wrote source_run_id/source_run_attempt from its own (validated) trigger
  // payload; re-validating the run from the GitHub API prevents a forged,
  // stale, or re-run id from redirecting the comment to a different PR build.
  let sourceRun;
  if (typeof github.rest.actions.getWorkflowRunAttempt === 'function') {
    try {
      ({ data: sourceRun } = await github.rest.actions.getWorkflowRunAttempt({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: sourceRunId,
        attempt_number: sourceRunAttempt,
      }));
    } catch (error) {
      core.warning(`Could not resolve source build run #${sourceRunId} attempt ${sourceRunAttempt}: ${error.message}`);
      return skip(core, 'Could not resolve the original source build run; skipping.');
    }
  } else {
    // Older @actions/github runtimes lack getWorkflowRunAttempt; fall back to
    // the run endpoint and pin the attempt manually.
    try {
      ({ data: sourceRun } = await github.rest.actions.getWorkflowRun({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: sourceRunId,
      }));
    } catch (error) {
      core.warning(`Could not resolve source build run #${sourceRunId}: ${error.message}`);
      return skip(core, 'Could not resolve the original source build run; skipping.');
    }
    if (Number(sourceRun.run_attempt) !== sourceRunAttempt) {
      return skip(core, `Source build run attempt ${sourceRun.run_attempt} does not match metadata attempt ${sourceRunAttempt}; skipping.`);
    }
  }

  if (sourceRun.name !== SOURCE_WORKFLOW_NAME) {
    return skip(core, `Source build run belongs to ${JSON.stringify(sourceRun.name)}, not ${SOURCE_WORKFLOW_NAME}; skipping.`);
  }
  if (canonicalRunPath(sourceRun.path) !== SOURCE_WORKFLOW_PATH) {
    return skip(core, `Source build run is at ${JSON.stringify(sourceRun.path)}, not ${SOURCE_WORKFLOW_PATH}; skipping.`);
  }
  if (sourceRun.event !== 'pull_request') {
    return skip(core, `Source build run event is ${sourceRun.event}, not pull_request; skipping.`);
  }
  if (sourceRun.conclusion !== 'success') {
    return skip(core, `Source build run conclusion is ${sourceRun.conclusion}, not success; skipping.`);
  }
  if (String(sourceRun.head_sha).toLowerCase() !== String(metadata.head_sha).toLowerCase()) {
    return skip(core, 'Source build run head SHA differs from artifact metadata; skipping.');
  }
  const sourceHeadRepo = sourceRun.head_repository?.full_name;
  if (!sourceHeadRepo) {
    return skip(core, 'Source build run head repository is missing; skipping.');
  }
  if (sourceHeadRepo !== metadata.head_repo) {
    return skip(core, 'Source build run head repository differs from artifact metadata; skipping.');
  }

  // 3. PR membership: GitHub leaves workflow_run.pull_requests empty for fork
  // PRs. When present, use it as an extra guard; otherwise resolve the unique
  // open PR from the trusted source run head repo/branch/SHA metadata before
  // accepting the artifact PR.
  const workflowPrNumbers = new Set(
    (sourceRun.pull_requests || []).map(pr => Number(pr.number)).filter(isPositiveInt)
  );
  if (workflowPrNumbers.size > 0) {
    if (!workflowPrNumbers.has(prNumber)) {
      return skip(core, `PR #${prNumber} is not listed in source build run ${sourceRunId}; skipping.`);
    }
  } else {
    const runHeadOwner = sourceRun.head_repository?.owner?.login;
    const runHeadBranch = sourceRun.head_branch;
    if (!runHeadOwner || !runHeadBranch) {
      return skip(core, 'Source build run head owner or branch is missing; skipping.');
    }

    let matchingPrs;
    try {
      const { data: pullRequests } = await github.rest.pulls.list({
        owner: context.repo.owner,
        repo: context.repo.repo,
        state: 'open',
        head: `${runHeadOwner}:${runHeadBranch}`,
        per_page: 100,
      });
      matchingPrs = pullRequests.filter(pr => (
        pr.head.repo?.full_name === sourceHeadRepo &&
        pr.head.sha === sourceRun.head_sha &&
        pr.base.repo?.full_name === metadata.base_repo
      ));
    } catch (error) {
      core.warning(`Could not resolve PR from source build run metadata: ${error.message}`);
      return skip(core, 'Could not resolve PR from source build run metadata; skipping.');
    }

    if (matchingPrs.length !== 1) {
      return skip(core, `Source build run matched ${matchingPrs.length} open PRs; skipping.`);
    }
    if (Number(matchingPrs[0].number) !== prNumber) {
      return skip(core, `Artifact PR #${prNumber} does not match source build run PR #${matchingPrs[0].number}; skipping.`);
    }
  }

  // 4. Current PR state: must still be open, on the same base/head
  // repositories, and at the same head SHA as the source run (stale/moved PR
  // rejection).
  let pull;
  try {
    ({ data: pull } = await github.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: prNumber,
    }));
  } catch (error) {
    core.warning(`Could not read PR #${prNumber}: ${error.message}`);
    return skip(core, `Could not read PR #${prNumber}; skipping.`);
  }

  if (pull.state !== 'open') {
    return skip(core, `PR #${prNumber} is ${pull.state}; skipping.`);
  }
  if (pull.base.repo.full_name !== metadata.base_repo || pull.head.repo.full_name !== metadata.head_repo) {
    return skip(core, 'Current PR repository metadata does not match artifact; skipping.');
  }
  if (pull.head.sha !== metadata.head_sha) {
    return skip(core, 'Current PR head SHA differs from artifact; skipping stale run.');
  }

  const reportPaths = findReports(artifactDir);
  const serverUrl = process.env.GITHUB_SERVER_URL || 'https://github.com';
  let body = [
    '## Query regression report',
    '',
    '> Rendered by a trusted workflow from JSON artifacts produced by the query-regression run. Results from untrusted PR code are advisory until reviewed.',
    '',
    `- **Controller run:** ${serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${controllerRunId}`,
    `- **Source build run:** ${serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${sourceRunId}`,
    `- **Built base SHA:** \`${text(metadata.built_base_sha)}\``,
    `- **Event base SHA:** \`${text(metadata.event_base_sha)}\``,
    `- **Head SHA:** \`${text(metadata.head_sha)}\``,
    `- **Candidate merge SHA:** \`${text(metadata.candidate_sha)}\``,
    '',
  ].join('\n');

  if (reportPaths.length === 0) {
    body += 'No query-regression JSON reports were found in the artifact.\n';
  } else {
    const rows = [];
    for (const reportPath of reportPaths) {
      let report;
      try {
        report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
      } catch (error) {
        return skip(core, `Invalid report JSON in ${reportPath}: ${error.message}`);
      }
      rows.push(...collectReportRows(report, reportPath));
    }
    body += renderSummaryTable(rows) + '\n';
  }

  fs.writeFileSync(summaryPath, body);

  core.setOutput('should_post', 'true');
  core.setOutput('pr_number', String(prNumber));
  core.setOutput('summary_path', summaryPath);
};

module.exports.SOURCE_WORKFLOW_NAME = SOURCE_WORKFLOW_NAME;
module.exports.CONTROLLER_WORKFLOW_NAME = CONTROLLER_WORKFLOW_NAME;
module.exports.SOURCE_WORKFLOW_PATH = SOURCE_WORKFLOW_PATH;
module.exports.CONTROLLER_WORKFLOW_PATH = CONTROLLER_WORKFLOW_PATH;
module.exports.canonicalRunPath = canonicalRunPath;
module.exports._test = { collectReportRows, renderSummaryTable };
