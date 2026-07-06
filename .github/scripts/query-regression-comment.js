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
  return String(value)
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/@/g, '@\u200b')
    .replace(/\|/g, '\\|')
    .replace(/\r?\n/g, ' ');
}

function statusEmoji(status) {
  return { ok: '✅', measured: '✅', failed: '❌', planned: '📝', 'fixture-ready': '🧪' }[status] || '⚠️';
}

function fmtMs(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(2) : 'N/A';
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
  const b = Number(base);
  const c = Number(candidate);
  if (!Number.isFinite(b) || !Number.isFinite(c) || b === 0) return 'N/A';
  return `${(((c - b) / b) * 100).toFixed(1)}%`;
}

function renderReport(report, reportPath) {
  const caseInfo = report.case || {};
  const name = caseInfo.name || path.basename(path.dirname(reportPath));
  const status = report.status || 'missing';
  const lines = [
    `### ${statusEmoji(status)} ${text(name)}`,
    '',
    `- **Status:** \`${text(status)}\``,
    `- **Case path:** \`${text(report.case_path)}\``,
    `- **Query mode:** \`${text(report.query_mode)}\``,
  ];
  if (report.error) {
    lines.push(`- **Error:** \`${text(report.error)}\``);
  }

  const targets = Array.isArray(report.targets) ? report.targets : [];
  lines.push('', '| Target | Status | Validation errors | Region |', '| --- | --- | ---: | --- |');
  for (const target of targets) {
    const discovered = target?.discovered || {};
    const region = Array.isArray(discovered)
      ? discovered.map(item => item?.region_id).filter(Boolean).join(', ')
      : discovered.region_id;
    lines.push(
      `| ${text(target?.name)} | ${statusEmoji(target?.status)} \`${text(target?.status)}\` | ${(target?.validation_errors || []).length} | ${text(region)} |`
    );
  }

  if (targets.length >= 2) {
    const base = measurementsByName(targets[0]);
    const candidate = measurementsByName(targets[1]);
    const names = Array.from(new Set([...base.keys(), ...candidate.keys()])).sort();
    lines.push('', '| Query | Base median ms | Candidate median ms | Regression |', '| --- | ---: | ---: | ---: |');
    for (const query of names) {
      const b = base.get(query) || {};
      const c = candidate.get(query) || {};
      lines.push(
        `| ${text(query)} | ${fmtMs(b.latency_ms_median)} | ${fmtMs(c.latency_ms_median)} | ${regression(b.latency_ms_median, c.latency_ms_median)} |`
      );
    }
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
  const trustedPrNumbers = new Set(
    (run.pull_requests || []).map(pr => Number(pr.number)).filter(Number.isInteger)
  );
  try {
    const { data: associatedPrs } = await github.rest.repos.listPullRequestsAssociatedWithCommit({
      owner: context.repo.owner,
      repo: context.repo.repo,
      commit_sha: run.head_sha,
    });
    for (const pr of associatedPrs) {
      trustedPrNumbers.add(Number(pr.number));
    }
  } catch (error) {
    core.warning(`Could not list PRs associated with workflow_run head SHA: ${error.message}`);
  }
  if (!trustedPrNumbers.has(prNumber)) {
    return skip(core, `PR #${prNumber} is not associated with workflow_run ${run.id}; skipping.`);
  }

  const { data: pull } = await github.rest.pulls.get({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: prNumber,
  });

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
    `- **Head SHA:** \`${text(metadata.head_sha)}\``,
    '',
  ].join('\n');

  if (reportPaths.length === 0) {
    body += 'No query-regression JSON reports were found in the artifact.\n';
  } else {
    const rendered = [];
    for (const reportPath of reportPaths) {
      const size = fs.statSync(reportPath).size;
      if (size > 1024 * 1024) {
        return skip(core, `Report file too large: ${reportPath} (${size} bytes).`);
      }
      let report;
      try {
        report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
      } catch (error) {
        return skip(core, `Invalid report JSON in ${reportPath}: ${error.message}`);
      }
      rendered.push(renderReport(report, reportPath));
    }
    body += rendered.join('\n\n---\n\n') + '\n';
  }

  if (body.length > 59999) {
    body = `${body.slice(0, 59500)}\n\n…\n\n_Comment truncated because it exceeded 60000 characters._\n`;
  }
  fs.writeFileSync(summaryPath, body);

  core.setOutput('should_post', 'true');
  core.setOutput('pr_number', String(prNumber));
  core.setOutput('summary_path', summaryPath);
};
