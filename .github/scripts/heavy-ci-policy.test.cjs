const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const assert = require('node:assert/strict');
const { test } = require('node:test');
const workflows = resolve(__dirname, '../workflows');
const integration = readFileSync(resolve(workflows, 'integration.yml'), 'utf8');
const suites = ['fuzztest', 'unstable-fuzztest', 'distributed-fuzztest', 'distributed-fuzztest-with-chaos'];
const prerequisites = ['prepare-fuzz-targets', 'build-greptime-ci'];
function enabled(job, { event = 'pull_request', action = 'synchronize', draft = false, attempt = 1, profile = 'none', repository = 'GreptimeTeam/greptimedb' } = {}) {
  const expression = integration.match(new RegExp(`^  ${job}:\\n    if: \\$\\{\\{ (.+) \\}\\}`, 'm'))[1];
  // These job guards use only operators whose semantics match JS for this fixture.
  return new Function('github', 'inputs', `return (${expression});`)(
    { repository, event_name: event, event: { action, pull_request: { draft } }, run_attempt: attempt },
    { fuzz_profile: profile, ci_command: false }
  );
}
test('automatic full fuzz on ready PR admission and schedule, not every update', () => {
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
