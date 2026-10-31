const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const assert = require('node:assert/strict');
const { test } = require('node:test');
const workflows = resolve(__dirname, '../workflows');
const integration = readFileSync(resolve(workflows, 'integration.yml'), 'utf8');
const suites = ['fuzztest', 'unstable-fuzztest', 'distributed-fuzztest', 'distributed-fuzztest-with-chaos'];
const prerequisites = ['prepare-fuzz-targets', 'build-greptime-ci'];
function enabled(job, { event = 'pull_request', action = 'synchronize', draft = false, attempt = 1, profile = 'none', repository = 'GreptimeTeam/greptimedb', outputs = {}, results = {}, cancelled = false, workflow = integration } = {}) {
  const block = workflow.split(`  ${job}:\n`)[1].split(/\n  [a-z][\w-]*:/)[0];
  if (block.includes("needs['github-script-tests']")) {
    assert.match(block, /^    needs: \[.*github-script-tests.*\]/m);
  }
  const expression = block.match(/^    if: \$\{\{ (.+) \}\}/m)[1];
  // These job guards use only operators whose semantics match JS for this fixture.
  return new Function('github', 'inputs', 'needs', 'cancelled', `return (${expression});`)(
    { repository, event_name: event, event: { action, pull_request: { draft } }, run_attempt: attempt },
    { fuzz_profile: profile, ci_command: false },
    { scope: { outputs }, ...Object.fromEntries(['github-script-tests', 'build', ...prerequisites].map(name => [name, { result: results[name] || 'success' }])) },
    () => cancelled
  );
}
test('fuzz event policy on ready PR admission and schedule, not every update', () => {
  for (const job of [...suites, ...prerequisites]) {
    for (const action of ['opened', 'ready_for_review']) assert.equal(enabled(job, { action }), true, job);
    for (const action of ['synchronize', 'reopened']) assert.equal(enabled(job, { action }), false, job);
    assert.equal(enabled(job, { event: 'schedule' }), true, job);
    assert.equal(enabled(job, { event: 'merge_group' }), false, job);
    assert.equal(enabled(job, { attempt: 2 }), false, job);
    assert.equal(enabled(job, { action: 'opened', draft: true }), false, job);
    assert.equal(enabled(job, { attempt: 2, draft: true }), false, job);
    assert.equal(enabled(job, { event: 'schedule', repository: 'fork/greptimedb' }), false, job);
  }
});
test('manual profiles select only their suite and all required build jobs', () => {
  const expected = { none: [], standalone: suites.slice(0, 2), distributed: [suites[2]], chaos: [suites[3]], all: suites };
  for (const [profile, jobs] of Object.entries(expected)) {
    assert.deepEqual(suites.filter(job => enabled(job, { event: 'workflow_dispatch', profile })), jobs);
    for (const job of prerequisites) assert.equal(enabled(job, { event: 'workflow_dispatch', profile }), profile !== 'none');
  }
});
test('core integration tests still run on PR updates and merge groups', () => {
  for (const job of ['build', 'sqlness', 'export-import-v2-e2e', 'run-multi-lang-tests']) {
    assert.equal(enabled(job), true, job);
    assert.equal(enabled(job, { event: 'merge_group' }), true, job);
  }
});
test('Windows compilation stays in Rust CI; test coverage remains in Nightly CI', () => {
  const rust = readFileSync(resolve(workflows, 'rust.yml'), 'utf8');
  const nightly = readFileSync(resolve(workflows, 'nightly-ci.yml'), 'utf8');
  assert.match(rust, /^  check-windows:/m);
  assert.doesNotMatch(rust, /^  test-windows:/m);
  assert.match(nightly, /^  test-on-windows:/m);
  assert.match(nightly, /^  sqlness-windows:/m);
  assert.match(nightly, /cargo nextest run -F dashboard/);
  assert.match(nightly, /^  workflow_dispatch:/m);
});

test('scope exclusions and dependency failures compose with the event policy', () => {
  const gates = {
    'prepare-fuzz-targets': 'any_fuzz', 'build-greptime-ci': 'build_ci_needed',
    fuzztest: 'fuzz_standalone', 'unstable-fuzztest': 'fuzz_unstable',
    'distributed-fuzztest': 'fuzz_distributed', 'distributed-fuzztest-with-chaos': 'fuzz_chaos'
  };
  for (const [job, gate] of Object.entries(gates)) {
    assert.equal(enabled(job, { action: 'opened', outputs: { [gate]: 'false' } }), false, job);
    assert.equal(enabled(job, { action: 'opened', outputs: {} }), true, job);
    assert.equal(enabled(job, { outputs: { [gate]: 'true' } }), false, job);
    assert.equal(enabled(job, { action: 'opened', cancelled: true }), false, job);
  }
  for (const job of suites) {
    for (const result of ['failure', 'skipped', 'cancelled']) {
      assert.equal(enabled(job, { action: 'opened', results: { 'prepare-fuzz-targets': result } }), false, job);
      const build = job === 'fuzztest' ? 'build' : 'build-greptime-ci';
      assert.equal(enabled(job, { action: 'opened', results: { [build]: result } }), false, job);
    }
  }
});

test('script tests must pass before any expensive integration job can start', () => {
  const jobs = ['build', 'sqlness', 'sqlness-kvbackend', 'export-import-v2-e2e',
    'run-multi-lang-tests', 'compat', ...prerequisites, ...suites];
  for (const job of jobs) {
    assert.equal(enabled(job, { action: 'opened' }), true, job);
    for (const result of ['failure', 'skipped', 'cancelled']) {
      assert.equal(enabled(job, { action: 'opened', results: { 'github-script-tests': result } }), false, job);
    }
  }
});

test('script tests gate expensive Rust and Checks jobs', () => {
  for (const [file, jobs] of [
    ['rust.yml', ['clippy', 'check-riscv64', 'check-windows', 'test', 'coverage']],
    ['checks.yml', ['check']]
  ]) {
    const workflow = readFileSync(resolve(workflows, file), 'utf8');
    for (const job of jobs) {
      const event = job === 'coverage' ? 'merge_group' : 'workflow_dispatch';
      assert.equal(enabled(job, { workflow, event }), true, job);
      for (const result of ['failure', 'skipped', 'cancelled']) {
        assert.equal(enabled(job, { workflow, event, results: { 'github-script-tests': result } }), false, job);
      }
    }
  }
});
