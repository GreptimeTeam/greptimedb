const fs = require('fs');
const os = require('os');
const path = require('path');
const test = require('node:test');
const assert = require('node:assert/strict');

const handler = require('./query-regression-comment.cjs');
const { collectReportRows, renderSummaryTable } = handler._test;

function report(name, measurements, thresholds = []) {
  return {
    case: { name },
    status: 'ok',
    targets: [
      { measurements: measurements.base },
      { measurements: measurements.candidate },
    ],
    thresholds,
  };
}

test('keeps the default export callable and exposes only the test seam', () => {
  assert.equal(typeof handler, 'function');
  assert.equal(handler.constructor.name, 'AsyncFunction');
  assert.deepEqual(Object.keys(handler._test).sort(), ['collectReportRows', 'renderSummaryTable']);
});

test('renders every case in one summary table without per-case separators', () => {
  const rows = [
    ...collectReportRows(report('first', {
      base: [{ name: 'q1', latency_ms_median: 10 }],
      candidate: [{ name: 'q1', latency_ms_median: 11 }],
    }), '/reports/first/query-regression-report.json'),
    ...collectReportRows(report('second', {
      base: [{ name: 'q2', latency_ms_median: 20 }],
      candidate: [{ name: 'q2', latency_ms_median: 18 }],
    }), '/reports/second/query-regression-report.json'),
  ];

  const table = renderSummaryTable(rows);

  assert.equal((table.match(/^\| Case \| Query \| Case status \|/gm) || []).length, 1);
  assert.match(table, /\| first \| q1 \|/);
  assert.match(table, /\| second \| q2 \|/);
  assert.doesNotMatch(table, /^### /m);
  assert.doesNotMatch(table, /^---$/m);
});

test('renders failed reports as N/A rows with their error', () => {
  const rows = collectReportRows({
    case: { name: 'broken' },
    status: 'failed',
    error: 'connection refused',
  }, '/reports/broken/query-regression-report.json');

  assert.deepEqual(rows, [{
    caseName: 'broken',
    query: 'N/A',
    status: 'failed',
    baseMedian: 'N/A',
    candidateMedian: 'N/A',
    regression: 'N/A',
    threshold: 'error: connection refused',
  }]);
});

test('reports missing targets and empty measurements', () => {
  const missingTargets = collectReportRows({ case: { name: 'missing' }, status: 'failed' }, '/reports/missing/report.json');
  const emptyMeasurements = collectReportRows(report('empty', { base: [], candidate: [] }), '/reports/empty/report.json');

  assert.equal(missingTargets[0].threshold, 'base/candidate measurements missing');
  assert.equal(emptyMeasurements[0].threshold, 'no query measurements found');
});

test('renders null, array, and primitive reports as invalid report rows', () => {
  for (const invalidReport of [null, [], 'not an object']) {
    assert.deepEqual(
      collectReportRows(invalidReport, '/reports/fallback/query-regression-report.json'),
      [{
        caseName: 'fallback',
        query: 'N/A',
        status: 'missing',
        baseMedian: 'N/A',
        candidateMedian: 'N/A',
        regression: 'N/A',
        threshold: 'invalid report object',
      }]
    );
  }
});

test('rejects null medians and diagnoses missing asymmetric measurements', () => {
  const rows = collectReportRows(report('missing-values', {
    base: [
      { name: 'null-median', latency_ms_median: null },
      { name: 'base-only', latency_ms_median: 10 },
      { name: 'candidate-null', latency_ms_median: 20 },
    ],
    candidate: [
      { name: 'null-median', latency_ms_median: 20 },
      { name: 'candidate-null', latency_ms_median: null },
      { name: 'candidate-only', latency_ms_median: 30 },
    ],
  }), '/reports/missing-values/query-regression-report.json');
  const byQuery = new Map(rows.map(row => [row.query, row]));

  assert.equal(byQuery.get('null-median').baseMedian, 'N/A');
  assert.equal(byQuery.get('null-median').candidateMedian, '20.00');
  assert.equal(byQuery.get('null-median').regression, 'N/A');
  assert.equal(byQuery.get('null-median').threshold, 'base measurement missing');
  assert.equal(byQuery.get('base-only').threshold, 'candidate measurement missing');
  assert.equal(byQuery.get('candidate-null').candidateMedian, 'N/A');
  assert.equal(byQuery.get('candidate-null').regression, 'N/A');
  assert.equal(byQuery.get('candidate-null').threshold, 'candidate measurement missing');
  assert.equal(byQuery.get('candidate-only').threshold, 'base measurement missing');
});

test('rejects empty, blank, NaN, and infinite medians', () => {
  const rows = collectReportRows(report('invalid-values', {
    base: [
      { name: 'empty-base', latency_ms_median: '' },
      { name: 'blank-base', latency_ms_median: '   ' },
      { name: 'nan-candidate', latency_ms_median: 10 },
      { name: 'infinite-candidate', latency_ms_median: 10 },
    ],
    candidate: [
      { name: 'empty-base', latency_ms_median: 10 },
      { name: 'blank-base', latency_ms_median: 10 },
      { name: 'nan-candidate', latency_ms_median: Number.NaN },
      { name: 'infinite-candidate', latency_ms_median: Number.POSITIVE_INFINITY },
    ],
  }), '/reports/invalid-values/query-regression-report.json');
  const byQuery = new Map(rows.map(row => [row.query, row]));

  for (const query of ['empty-base', 'blank-base', 'nan-candidate', 'infinite-candidate']) {
    assert.equal(byQuery.get(query).regression, 'N/A');
  }
  assert.equal(byQuery.get('empty-base').baseMedian, 'N/A');
  assert.equal(byQuery.get('blank-base').baseMedian, 'N/A');
  assert.equal(byQuery.get('nan-candidate').candidateMedian, 'N/A');
  assert.equal(byQuery.get('infinite-candidate').candidateMedian, 'N/A');
});

test('rejects boolean, array, and object median coercions', () => {
  const rows = collectReportRows(report('invalid-types', {
    base: [
      { name: 'boolean-base', latency_ms_median: true },
      { name: 'array-base', latency_ms_median: [] },
      { name: 'object-base', latency_ms_median: {} },
      { name: 'boolean-candidate', latency_ms_median: 10 },
      { name: 'array-candidate', latency_ms_median: 10 },
      { name: 'object-candidate', latency_ms_median: 10 },
    ],
    candidate: [
      { name: 'boolean-base', latency_ms_median: 10 },
      { name: 'array-base', latency_ms_median: 10 },
      { name: 'object-base', latency_ms_median: 10 },
      { name: 'boolean-candidate', latency_ms_median: false },
      { name: 'array-candidate', latency_ms_median: [] },
      { name: 'object-candidate', latency_ms_median: {} },
    ],
  }), '/reports/invalid-types/query-regression-report.json');

  for (const row of rows) {
    assert.equal(row.regression, 'N/A');
  }
  assert.equal(rows.find(row => row.query === 'boolean-base').baseMedian, 'N/A');
  assert.equal(rows.find(row => row.query === 'array-base').baseMedian, 'N/A');
  assert.equal(rows.find(row => row.query === 'object-base').baseMedian, 'N/A');
  assert.equal(rows.find(row => row.query === 'boolean-candidate').candidateMedian, 'N/A');
  assert.equal(rows.find(row => row.query === 'array-candidate').candidateMedian, 'N/A');
  assert.equal(rows.find(row => row.query === 'object-candidate').candidateMedian, 'N/A');
});

test('sorts the base and candidate query union and aggregates scoped thresholds', () => {
  const rows = collectReportRows(report('union', {
    base: [{ name: 'z', latency_ms_median: 10 }],
    candidate: [{ name: 'a', latency_ms_median: 20 }],
  }, [
    { query: 'z', threshold: 'p95', target: 'base', status: 'warn' },
    { query: 'z', threshold: 'absolute', target: 'candidate', encoding: 'plain', status: 'pass' },
  ]), '/reports/union/query-regression-report.json');

  assert.deepEqual(rows.map(row => row.query), ['a', 'z']);
  assert.equal(rows[0].baseMedian, 'N/A');
  assert.equal(rows[0].candidateMedian, '20.00');
  assert.equal(rows[0].threshold, 'base measurement missing');
  assert.equal(
    rows[1].threshold,
    'candidate measurement missing; p95 [target=base]: warn, absolute [target=candidate, encoding=plain]: pass'
  );
});

test('preserves unscoped and unmatched thresholds in a synthetic N/A row', () => {
  const rows = collectReportRows(report('thresholds', {
    base: [{ name: 'measured', latency_ms_median: 10 }],
    candidate: [{ name: 'measured', latency_ms_median: 11 }],
  }, [
    { query: 'measured', threshold: 'query limit', target: 'base', status: 'passed' },
    { threshold: 'min_files', target: 'base', status: 'passed' },
    { threshold: 'min_files', target: 'candidate', status: 'failed' },
    { threshold: 'encoding limit', target: 'candidate', encoding: 'plain', status: 'failed' },
    {
      query: 'not-measured',
      threshold: 'orphaned limit',
      target: 'base',
      encoding: 'json',
      status: 'failed',
      reason: 'measurement unavailable',
    },
  ]), '/reports/thresholds/query-regression-report.json');

  assert.equal(rows.length, 2);
  assert.equal(rows[0].query, 'measured');
  assert.equal(rows[0].threshold, 'query limit [target=base]: passed');
  assert.equal(rows[1].query, 'N/A');
  assert.equal(
    rows[1].threshold,
    'case/storage threshold: min_files [target=base]: passed, min_files [target=candidate]: failed, encoding limit [target=candidate, encoding=plain]: failed; unmatched query not-measured: orphaned limit [target=base, encoding=json]: failed (reason: measurement unavailable)'
  );
});

test('keeps unscoped thresholds out of undefined and null query rows', () => {
  const rows = collectReportRows(report('collisions', {
    base: [
      { name: 'undefined', latency_ms_median: 10 },
      { name: 'null', latency_ms_median: 10 },
    ],
    candidate: [
      { name: 'undefined', latency_ms_median: 11 },
      { name: 'null', latency_ms_median: 11 },
    ],
  }, [
    { threshold: 'min_files', target: 'base', status: 'passed' },
    { query: null, threshold: 'min_files', target: 'candidate', status: 'failed' },
  ]), '/reports/collisions/query-regression-report.json');
  const byQuery = new Map(rows.map(row => [row.query, row]));

  assert.equal(byQuery.get('undefined').threshold, 'N/A');
  assert.equal(byQuery.get('null').threshold, 'N/A');
  assert.equal(rows.filter(row => row.query === 'N/A').length, 1);
  assert.equal(
    byQuery.get('N/A').threshold,
    'case/storage threshold: min_files [target=base]: passed, min_files [target=candidate]: failed'
  );
});

test('escapes Markdown table content, including bare carriage returns', () => {
  const table = renderSummaryTable([{
    caseName: 'safe\r| injected |\n<!-- hidden -->@user <tag>',
    query: 'query`|\n@team',
    status: 'failed',
    baseMedian: '1|2',
    candidateMedian: '3\n4',
    regression: '<!-- comment -->`@all',
    threshold: 'x|y\r\n<!-- drop -->`@here <html>',
  }]);

  assert.equal(table.split('\n').length, 3);
  assert.match(table, /safe \\\| injected \\\| @\u200buser &lt;tag&gt;/);
  assert.match(table, /query&#96;\\\| @\u200bteam/);
  assert.match(table, /1\\\|2/);
  assert.match(table, /3 4/);
  assert.match(table, /&#96;@\u200ball/);
  assert.match(table, /x\\\|y &#96;@\u200bhere &lt;html&gt;/);
  assert.doesNotMatch(table, /hidden|comment|drop|\r/);
});

test('writes the explicit no-report summary without an empty table', async () => {
  const originalCwd = process.cwd();
  const originalRunId = process.env.WORKFLOW_RUN_ID;
  const originalRunAttempt = process.env.WORKFLOW_RUN_ATTEMPT;
  const temporaryDir = fs.mkdtempSync(path.join(os.tmpdir(), 'query-regression-comment-'));
  const artifactDir = path.join(temporaryDir, 'query-regression-comment');
  const outputs = new Map();

  try {
    fs.mkdirSync(artifactDir);
    fs.writeFileSync(path.join(artifactDir, 'query-regression-pr.json'), JSON.stringify({
      run_id: 101,
      run_attempt: 1,
      base_repo: 'owner/repo',
      pr_number: 42,
      head_sha: 'head-sha',
      head_repo: 'fork/repo',
      built_base_sha: 'base-sha',
      event_base_sha: 'event-base-sha',
      candidate_sha: 'candidate-sha',
    }));
    process.chdir(temporaryDir);
    process.env.WORKFLOW_RUN_ID = '101';
    process.env.WORKFLOW_RUN_ATTEMPT = '1';

    await handler({
      core: {
        info() {},
        warning() {},
        setOutput(name, value) { outputs.set(name, value); },
      },
      context: {
        repo: { owner: 'owner', repo: 'repo' },
        payload: {
          workflow_run: {
            event: 'pull_request',
            head_sha: 'head-sha',
            head_repository: { full_name: 'fork/repo' },
            pull_requests: [{ number: 42 }],
          },
        },
      },
      github: {
        rest: {
          pulls: {
            get: async () => ({
              data: {
                state: 'open',
                base: { repo: { full_name: 'owner/repo' } },
                head: { repo: { full_name: 'fork/repo' }, sha: 'head-sha' },
              },
            }),
          },
        },
      },
    });

    const summary = fs.readFileSync(path.join(artifactDir, 'query-regression-summary.md'), 'utf8');
    assert.equal(outputs.get('should_post'), 'true');
    assert.match(summary, /No query-regression JSON reports were found in the artifact\./);
    assert.doesNotMatch(summary, /\| Case \| Query \|/);
  } finally {
    process.chdir(originalCwd);
    if (originalRunId === undefined) delete process.env.WORKFLOW_RUN_ID;
    else process.env.WORKFLOW_RUN_ID = originalRunId;
    if (originalRunAttempt === undefined) delete process.env.WORKFLOW_RUN_ATTEMPT;
    else process.env.WORKFLOW_RUN_ATTEMPT = originalRunAttempt;
    fs.rmSync(temporaryDir, { recursive: true, force: true });
  }
});
