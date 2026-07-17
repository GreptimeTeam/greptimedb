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
  return hits.length > 0 ? hits.join(', ') : 'N/A';
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

function syntheticThresholdStatus(thresholds, measurementNames) {
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

  const parts = [];
  if (unscoped.length > 0) {
    parts.push(`case/storage threshold: ${thresholdStatus(unscoped)}`);
  }
  for (const query of Array.from(unmatched.keys()).sort()) {
    parts.push(`unmatched query ${query}: ${thresholdStatus(unmatched.get(query))}`);
  }
  return parts.length > 0 ? parts.join('; ') : 'N/A';
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
  const syntheticThresholds = syntheticThresholdStatus(thresholds, measurementNames);
  if (syntheticThresholds !== 'N/A') {
    rows.push({
      caseName: name,
      query: 'N/A',
      status,
      baseMedian: 'N/A',
      candidateMedian: 'N/A',
      regression: 'N/A',
      threshold: syntheticThresholds,
    });
  }
  return rows;
}

function renderSummaryTable(rows) {
  const lines = [
    '| Case | Query | Case status | Base median ms | Candidate median ms | Regression | Threshold |',
    '| --- | --- | --- | ---: | ---: | ---: | --- |',
  ];
  for (const row of rows) {
    lines.push(
      `| ${text(row.caseName)} | ${text(row.query)} | ${statusEmoji(row.status)} \`${text(row.status)}\` | ${text(row.baseMedian)} | ${text(row.candidateMedian)} | ${text(row.regression)} | ${text(row.threshold)} |`
    );
  }
  return lines.join('\n');
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

  const expectedRunId = Number(process.env.WORKFLOW_RUN_ID);
  const expectedRunAttempt = Number(process.env.WORKFLOW_RUN_ATTEMPT);
  if (metadata.run_id !== expectedRunId || metadata.run_attempt !== expectedRunAttempt) {
    return skip(core, 'Artifact metadata does not match this workflow_run; skipping.');
  }

  if (metadata.base_repo !== `${context.repo.owner}/${context.repo.repo}`) {
    return skip(core, `PR targets ${metadata.base_repo}, not this repository; skipping.`);
  }

  const prNumber = Number(metadata.pr_number);
  if (!Number.isInteger(prNumber) || prNumber <= 0) {
    return skip(core, 'Invalid PR number in metadata; skipping.');
  }

  const run = context.payload.workflow_run;
  if (run.event !== 'pull_request') {
    return skip(core, `Workflow run event is ${run.event}, not pull_request; skipping.`);
  }
  if (run.head_sha !== metadata.head_sha) {
    return skip(core, 'Workflow run head SHA differs from artifact metadata; skipping.');
  }
  const runHeadRepo = run.head_repository?.full_name;
  if (!runHeadRepo) {
    return skip(core, 'Workflow run head repository is missing; skipping.');
  }
  if (runHeadRepo !== metadata.head_repo) {
    return skip(core, 'Workflow run head repository differs from artifact metadata; skipping.');
  }

  // GitHub leaves workflow_run.pull_requests empty for fork PRs. When present,
  // use it as an extra guard; otherwise resolve the unique open PR from trusted
  // workflow_run head repo/branch/SHA metadata before accepting the artifact PR.
  const workflowPrNumbers = new Set(
    (run.pull_requests || []).map(pr => Number(pr.number)).filter(Number.isInteger)
  );
  if (workflowPrNumbers.size > 0) {
    if (!workflowPrNumbers.has(prNumber)) {
      return skip(core, `PR #${prNumber} is not listed in workflow_run ${run.id}; skipping.`);
    }
  } else {
    const runHeadOwner = run.head_repository?.owner?.login;
    const runHeadBranch = run.head_branch;
    if (!runHeadOwner || !runHeadBranch) {
      return skip(core, 'Workflow run head owner or branch is missing; skipping.');
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
        pr.head.repo?.full_name === runHeadRepo &&
        pr.head.sha === run.head_sha &&
        pr.base.repo?.full_name === metadata.base_repo
      ));
    } catch (error) {
      core.warning(`Could not resolve PR from workflow_run metadata: ${error.message}`);
      return skip(core, 'Could not resolve PR from workflow_run metadata; skipping.');
    }

    if (matchingPrs.length !== 1) {
      return skip(core, `Workflow run matched ${matchingPrs.length} open PRs; skipping.`);
    }
    if (Number(matchingPrs[0].number) !== prNumber) {
      return skip(core, `Artifact PR #${prNumber} does not match workflow_run PR #${matchingPrs[0].number}; skipping.`);
    }
  }

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
    `- **Workflow run:** ${serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${expectedRunId}`,
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

module.exports._test = { collectReportRows, renderSummaryTable };
