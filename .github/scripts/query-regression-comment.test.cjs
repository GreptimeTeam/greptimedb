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

// ---------------------------------------------------------------------------
// Validation-chain harness. The comment workflow follows the trusted
// `Query Regression Controller` run; the controller followed the `Query
// Regression` build run and regenerated the comment metadata from validated
// values. The validator must pin the artifact to the immediate controller run
// and resolve/re-validate the original source build run from the metadata.
// ---------------------------------------------------------------------------

const HEAD_SHA = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
const BUILT_BASE_SHA = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
const EVENT_BASE_SHA = 'cccccccccccccccccccccccccccccccccccccccc';
const CANDIDATE_SHA = 'dddddddddddddddddddddddddddddddddddddddd';
const DEFAULT_BRANCH_SHA = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee';
const MOVED_SHA = 'ffffffffffffffffffffffffffffffffffffffff';

function defaultMetadata(overrides = {}) {
  return {
    run_id: 201,
    run_attempt: 2,
    source_run_id: 101,
    source_run_attempt: 1,
    base_repo: 'owner/repo',
    pr_number: 42,
    head_sha: HEAD_SHA,
    head_repo: 'fork/repo',
    built_base_sha: BUILT_BASE_SHA,
    event_base_sha: EVENT_BASE_SHA,
    candidate_sha: CANDIDATE_SHA,
    ...overrides,
  };
}

function defaultController(overrides = {}) {
  return {
    id: 201,
    attempt: 2,
    payload: {
      id: 201,
      name: 'Query Regression Controller',
      event: 'workflow_run',
      conclusion: 'success',
      path: '.github/workflows/query-regression-controller.yml',
      head_sha: DEFAULT_BRANCH_SHA,
      head_repository: { full_name: 'owner/repo' },
      pull_requests: [],
    },
    api: {
      id: 201,
      name: 'Query Regression Controller',
      event: 'workflow_run',
      conclusion: 'success',
      path: '.github/workflows/query-regression-controller.yml',
      head_sha: DEFAULT_BRANCH_SHA,
      head_repository: { full_name: 'owner/repo' },
    },
    ...overrides,
  };
}

function defaultSource(overrides = {}) {
  return {
    id: 101,
    attempt: 1,
    api: {
      id: 101,
      name: 'Query Regression',
      event: 'pull_request',
      conclusion: 'success',
      run_attempt: 1,
      path: '.github/workflows/query-regression.yml',
      head_sha: HEAD_SHA,
      head_branch: 'feature-branch',
      head_repository: { full_name: 'fork/repo', owner: { login: 'fork' } },
      pull_requests: [],
    },
    ...overrides,
  };
}

function defaultForkPr() {
  return {
    number: 42,
    head: { repo: { full_name: 'fork/repo' }, sha: HEAD_SHA },
    base: { repo: { full_name: 'owner/repo' } },
  };
}

function defaultOpenPull() {
  return {
    state: 'open',
    base: { repo: { full_name: 'owner/repo' } },
    head: { repo: { full_name: 'fork/repo' }, sha: HEAD_SHA },
  };
}

async function runValidator({
  metadata = defaultMetadata(),
  controller = defaultController(),
  source = defaultSource(),
  prs = [defaultForkPr()],
  pull = defaultOpenPull(),
  envRunId,
  envRunAttempt,
  useAttemptEndpoint = true,
  failSourceResolve = false,
} = {}) {
  const originalCwd = process.cwd();
  const originalRunId = process.env.WORKFLOW_RUN_ID;
  const originalRunAttempt = process.env.WORKFLOW_RUN_ATTEMPT;
  const temporaryDir = fs.mkdtempSync(path.join(os.tmpdir(), 'query-regression-comment-'));
  const artifactDir = path.join(temporaryDir, 'query-regression-comment');
  const outputs = new Map();
  const warnings = [];
  const calls = [];
  try {
    fs.mkdirSync(artifactDir);
    fs.writeFileSync(path.join(artifactDir, 'query-regression-pr.json'), JSON.stringify(metadata));
    process.chdir(temporaryDir);
    process.env.WORKFLOW_RUN_ID = String(envRunId === undefined ? controller.id : envRunId);
    process.env.WORKFLOW_RUN_ATTEMPT = String(envRunAttempt === undefined ? controller.attempt : envRunAttempt);

    const actions = {
      getWorkflowRun: async params => {
        calls.push({ endpoint: 'getWorkflowRun', params: { ...params } });
        if (controller && params.run_id === controller.id) return { data: controller.api };
        if (source && params.run_id === source.id) return { data: source.api };
        throw new Error('HTTP 404: run not found');
      },
    };
    if (useAttemptEndpoint) {
      actions.getWorkflowRunAttempt = async params => {
        calls.push({ endpoint: 'getWorkflowRunAttempt', params: { ...params } });
        if (failSourceResolve) throw new Error('HTTP 404: attempt not found');
        if (source && params.run_id === source.id && params.attempt_number === source.attempt) {
          return { data: source.api };
        }
        throw new Error('HTTP 404: attempt not found');
      };
    }
    const github = {
      rest: {
        actions,
        pulls: {
          list: async params => {
            calls.push({ endpoint: 'pulls.list', params: { ...params } });
            return { data: prs };
          },
          get: async params => {
            calls.push({ endpoint: 'pulls.get', params: { ...params } });
            return { data: pull };
          },
        },
      },
    };
    const context = {
      repo: { owner: 'owner', repo: 'repo' },
      payload: { workflow_run: controller.payload },
    };
    const core = {
      info() {},
      warning(message) { warnings.push(message); },
      setOutput(name, value) { outputs.set(name, value); },
    };

    await handler({ github, context, core });

    const summaryPath = outputs.get('summary_path');
    return {
      outputs: new Map(outputs),
      warnings,
      calls,
      summary: summaryPath ? fs.readFileSync(summaryPath, 'utf8') : null,
    };
  } finally {
    process.chdir(originalCwd);
    if (originalRunId === undefined) delete process.env.WORKFLOW_RUN_ID;
    else process.env.WORKFLOW_RUN_ID = originalRunId;
    if (originalRunAttempt === undefined) delete process.env.WORKFLOW_RUN_ATTEMPT;
    else process.env.WORKFLOW_RUN_ATTEMPT = originalRunAttempt;
    fs.rmSync(temporaryDir, { recursive: true, force: true });
  }
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
    'candidate measurement missing; p95 [target=base]: warn; absolute [target=candidate, encoding=plain]: pass'
  );
});

test('keeps unscoped and unmatched thresholds in a case-thresholds detail row', () => {
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
  assert.equal(rows[1].kind, 'case-thresholds');
  assert.equal(
    rows[1].threshold,
    'min_files [target=base]: passed; min_files [target=candidate]: failed; encoding limit [target=candidate, encoding=plain]: failed; unmatched query not-measured: orphaned limit [target=base, encoding=json]: failed (reason: measurement unavailable)'
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
  assert.equal(byQuery.get('N/A').kind, 'case-thresholds');
  assert.equal(
    byQuery.get('N/A').threshold,
    'min_files [target=base]: passed; min_files [target=candidate]: failed'
  );
});

test('renders case/storage thresholds in a collapsible details block below the table', () => {
  const rows = [
    ...collectReportRows(report('first', {
      base: [{ name: 'q1', latency_ms_median: 10 }],
      candidate: [{ name: 'q1', latency_ms_median: 11 }],
    }, [
      { query: 'q1', threshold: 'query limit', target: 'base', status: 'passed' },
      { threshold: 'min_files', target: 'base', status: 'passed' },
      { threshold: 'min_files', target: 'candidate', status: 'failed' },
    ]), '/reports/first/query-regression-report.json'),
    ...collectReportRows({
      case: { name: 'broken' },
      status: 'failed',
      error: 'connection refused',
    }, '/reports/broken/query-regression-report.json'),
  ];

  const rendered = renderSummaryTable(rows);
  const detailsIndex = rendered.indexOf('<details>');
  const table = rendered.slice(0, detailsIndex);
  const details = rendered.slice(detailsIndex);

  // Query rows and abnormal N/A rows stay in the main table.
  assert.match(table, /\| first \| q1 \| ✅ ok \|/);
  assert.match(table, /\| broken \| N\/A \| ❌ failed \|/);
  // Storage thresholds are not main-table rows.
  assert.doesNotMatch(table, /^\| first \| N\/A \|/m);
  assert.doesNotMatch(table, /min_files/);
  // Status column is plain text without code-span backticks.
  assert.doesNotMatch(table, /`/);
  // Details block keeps its tags unescaped and its items '; '-separated.
  assert.ok(details.startsWith('<details><summary>Case / storage thresholds</summary>'));
  assert.ok(details.includes('- first: min_files \\[target=base\\]: passed; min_files \\[target=candidate\\]: failed'));
  assert.ok(details.endsWith('</details>'));
  assert.ok(!rendered.includes('&lt;details&gt;'));
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

test('publishes only after validating the controller run, the source build run, and the PR', async () => {
  const result = await runValidator();

  assert.equal(result.outputs.get('should_post'), 'true');
  assert.equal(result.outputs.get('pr_number'), '42');
  assert.match(result.summary, /No query-regression JSON reports were found in the artifact\./);
  assert.doesNotMatch(result.summary, /\| Case \| Query \|/);
  // Both trusted runs are linked from the regenerated metadata.
  assert.match(result.summary, /Controller run:\*\* https:\/\/github\.com\/owner\/repo\/actions\/runs\/201/);
  assert.match(result.summary, /Source build run:\*\* https:\/\/github\.com\/owner\/repo\/actions\/runs\/101/);

  // The validator independently consulted the GitHub API for both runs, the
  // fork fallback, and the current PR state.
  assert.ok(result.calls.some(call => (
    call.endpoint === 'getWorkflowRun' && call.params.run_id === 201
  )));
  assert.ok(result.calls.some(call => (
    call.endpoint === 'getWorkflowRunAttempt' &&
    call.params.run_id === 101 &&
    call.params.attempt_number === 1
  )));
  assert.ok(result.calls.some(call => (
    call.endpoint === 'pulls.list' && call.params.head === 'fork:feature-branch'
  )));
  assert.ok(result.calls.some(call => (
    call.endpoint === 'pulls.get' && call.params.pull_number === 42
  )));
});

test('publishes when the source run lists the PR directly without the fork fallback', async () => {
  const source = defaultSource({
    api: { ...defaultSource().api, pull_requests: [{ number: 42 }] },
  });
  const result = await runValidator({ source, prs: [] });

  assert.equal(result.outputs.get('should_post'), 'true');
  assert.ok(!result.calls.some(call => call.endpoint === 'pulls.list'));
  assert.ok(result.calls.some(call => call.endpoint === 'pulls.get'));
});

test('publishes via the run-endpoint fallback when the attempt endpoint is unavailable', async () => {
  const result = await runValidator({ useAttemptEndpoint: false });

  assert.equal(result.outputs.get('should_post'), 'true');
  assert.ok(result.calls.some(call => (
    call.endpoint === 'getWorkflowRun' && call.params.run_id === 101
  )));
});

test('rejects a comment artifact replayed from another controller run', async () => {
  const result = await runValidator({ envRunId: 999 });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
  assert.ok(!result.calls.some(call => call.endpoint === 'getWorkflowRun'));
});

test('rejects metadata without valid source_run_id/source_run_attempt', async () => {
  const result = await runValidator({
    metadata: defaultMetadata({ source_run_id: undefined, source_run_attempt: undefined }),
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects metadata with forged source_run_id/source_run_attempt values', async () => {
  const result = await runValidator({
    metadata: defaultMetadata({ source_run_id: 'not-a-number', source_run_attempt: 0 }),
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a controller run that was not triggered by workflow_run', async () => {
  const base = defaultController();
  const result = await runValidator({
    controller: {
      ...base,
      payload: { ...base.payload, event: 'push' },
      api: { ...base.api, event: 'push' },
    },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a controller run from a different workflow', async () => {
  const base = defaultController();
  const result = await runValidator({
    controller: {
      ...base,
      payload: { ...base.payload, name: 'Other Workflow' },
      api: { ...base.api, name: 'Other Workflow' },
    },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('publishes when the controller run path is unqualified or ref-qualified', async () => {
  // REST/workflow_run payload `path` appends the triggering ref
  // (`.github/workflows/x.yml@refs/...`); the validator canonicalizes by
  // stripping the first `@ref` suffix and requires the exact controller
  // workflow file.
  for (const path of [
    '.github/workflows/query-regression-controller.yml', // unqualified
    '.github/workflows/query-regression-controller.yml@refs/heads/main', // branch-qualified
    '.github/workflows/query-regression-controller.yml@refs/tags/v1.2.3', // tag-qualified
    `.github/workflows/query-regression-controller.yml@${DEFAULT_BRANCH_SHA}`, // SHA-qualified
  ]) {
    const base = defaultController();
    const result = await runValidator({
      controller: {
        ...base,
        payload: { ...base.payload, path },
        api: { ...base.api, path },
      },
    });
    assert.equal(result.outputs.get('should_post'), 'true');
  }
});

test('publishes when the source build run path is unqualified or ref-qualified', async () => {
  for (const path of [
    '.github/workflows/query-regression.yml', // unqualified
    '.github/workflows/query-regression.yml@refs/heads/feature-branch', // branch-qualified
    '.github/workflows/query-regression.yml@refs/tags/v1.2.3', // tag-qualified
    `.github/workflows/query-regression.yml@${HEAD_SHA}`, // SHA-qualified
  ]) {
    const base = defaultSource();
    const result = await runValidator({
      source: { ...base, api: { ...base.api, path } },
    });
    assert.equal(result.outputs.get('should_post'), 'true');
  }
});

test('rejects a controller run whose workflow file path is not the controller workflow', async () => {
  // Exact canonical path is required: a different workflow file fails even
  // when the display name is correct (path is the canonical identity).
  for (const path of [
    '.github/workflows/evil.yml',
    '.github/workflows/evil.yml@refs/heads/main',
    '.github/workflows/query-regression.yml@refs/heads/main',
  ]) {
    const base = defaultController();
    const result = await runValidator({
      controller: {
        ...base,
        payload: { ...base.payload, path },
        api: { ...base.api, path },
      },
    });
    assert.equal(result.outputs.get('should_post'), 'false');
    assert.equal(result.outputs.has('summary_path'), false);
  }
});

test('rejects a source build run whose workflow file path is not the Query Regression workflow', async () => {
  for (const path of [
    '.github/workflows/evil.yml',
    '.github/workflows/evil.yml@refs/heads/main',
    '.github/workflows/query-regression-controller.yml@refs/heads/main',
  ]) {
    const base = defaultSource();
    const result = await runValidator({
      source: { ...base, api: { ...base.api, path } },
    });
    assert.equal(result.outputs.get('should_post'), 'false');
    assert.equal(result.outputs.has('summary_path'), false);
  }
});

test('canonicalizes REST run paths by stripping the first @ref suffix', () => {
  assert.equal(handler.canonicalRunPath('.github/workflows/query-regression.yml'), '.github/workflows/query-regression.yml');
  assert.equal(handler.canonicalRunPath('.github/workflows/query-regression-controller.yml@refs/heads/main'), '.github/workflows/query-regression-controller.yml');
  assert.equal(handler.canonicalRunPath('.github/workflows/query-regression-controller.yml@refs/tags/v1.2.3'), '.github/workflows/query-regression-controller.yml');
  assert.equal(handler.canonicalRunPath(`.github/workflows/query-regression.yml@${HEAD_SHA}`), '.github/workflows/query-regression.yml');
  assert.equal(handler.SOURCE_WORKFLOW_NAME, 'Query Regression');
  assert.equal(handler.CONTROLLER_WORKFLOW_NAME, 'Query Regression Controller');
  assert.equal(handler.SOURCE_WORKFLOW_PATH, '.github/workflows/query-regression.yml');
  assert.equal(handler.CONTROLLER_WORKFLOW_PATH, '.github/workflows/query-regression-controller.yml');
});

test('rejects a controller run whose head repository is not the base repository', async () => {
  const base = defaultController();
  const result = await runValidator({
    controller: {
      ...base,
      payload: { ...base.payload, head_repository: { full_name: 'evil/repo' } },
      api: { ...base.api, head_repository: { full_name: 'evil/repo' } },
    },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects when the controller run cannot be verified via the API', async () => {
  const base = defaultController();
  const result = await runValidator({
    controller: { ...base, id: 999 },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects an unresolvable source build run', async () => {
  const result = await runValidator({ failSourceResolve: true });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a source run that is not the Query Regression workflow', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, name: 'Other Build' } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a source run that is not a pull_request event', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, event: 'push' } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a source run that did not conclude successfully', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, conclusion: 'failure' } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a source run whose head SHA differs from the metadata', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, head_sha: MOVED_SHA } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a source run whose head repository differs from the metadata (fork spoof)', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, head_repository: { full_name: 'other/fork' } } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects when the source run attempt does not exist', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, attempt: 2, api: { ...base.api, run_attempt: 2 } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a re-run attempt via the fallback when the attempt endpoint is unavailable', async () => {
  const base = defaultSource();
  const result = await runValidator({
    useAttemptEndpoint: false,
    source: { ...base, api: { ...base.api, run_attempt: 2 } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects when the artifact PR is not in the source run pull_requests', async () => {
  const base = defaultSource();
  const result = await runValidator({
    source: { ...base, api: { ...base.api, pull_requests: [{ number: 43 }] } },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects when the fork fallback does not uniquely resolve the PR', async () => {
  const result = await runValidator({
    prs: [defaultForkPr(), { ...defaultForkPr(), number: 43 }],
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects when the fork fallback resolves a different PR', async () => {
  const result = await runValidator({
    prs: [{ ...defaultForkPr(), number: 43 }],
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a closed PR', async () => {
  const result = await runValidator({
    pull: { ...defaultOpenPull(), state: 'closed' },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a PR whose head SHA moved after the source run (stale PR)', async () => {
  const result = await runValidator({
    pull: {
      ...defaultOpenPull(),
      head: { repo: { full_name: 'fork/repo' }, sha: MOVED_SHA },
    },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});

test('rejects a PR whose head or base repository changed', async () => {
  const result = await runValidator({
    pull: {
      ...defaultOpenPull(),
      head: { repo: { full_name: 'other/fork' }, sha: HEAD_SHA },
    },
  });

  assert.equal(result.outputs.get('should_post'), 'false');
  assert.equal(result.outputs.has('summary_path'), false);
});
