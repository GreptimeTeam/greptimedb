const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const { test } = require('node:test');

const directory = resolve(__dirname, '../../workflows');
const workflows = ['checks', 'rust', 'integration', 'nightly-ci'].map(name => ({
  name,
  text: readFileSync(resolve(directory, name + '.yml'), 'utf8'),
}));
function jobs(text) {
  return [...text.matchAll(/^  ([\w-]+):\n([\s\S]*?)(?=^  [\w-]+:\n|(?![\s\S]))/gm)];
}
function steps(body) {
  return body.split(/^      - /m).slice(1);
}

test('build caches have one controller and lightweight checks do not cache targets', () => {
  let checked = 0;
  for (const { name, text } of workflows) {
    for (const [, job, body] of jobs(text)) {
      if (!body.includes('Swatinem/rust-cache@') && !['fmt', 'check-udeps', 'toml'].includes(job)) continue;
      for (const step of steps(body).filter(s => s.includes('uses: actions-rust-lang/setup-rust-toolchain@'))) {
        assert.match(step, /^          cache: false$/m, name + '/' + job);
        checked++;
      }
    }
  }
  assert.ok(checked >= 10, 'expected build and lightweight jobs to be covered');
});

test('only main writes caches, and the nightly read-only cache stays read-only', () => {
  let checked = 0;
  for (const { name, text } of workflows) {
    for (const [, job, body] of jobs(text)) {
      for (const step of steps(body)) {
        if (!step.includes('uses: Swatinem/rust-cache@') && !step.includes('uses: actions/cache/save@')) continue;
        const condition = step.includes('uses: Swatinem/rust-cache@')
          ? step.match(/^          save-if: (.+)$/m)?.[1]
          : step.match(/^        if: (.+)$/m)?.[1];
        assert.ok(condition, name + '/' + job);
        const expression = condition.replace(/^\$\{\{\s*|\s*\}\}$/g, '')
          .replaceAll('steps.fuzz-binaries.outputs.cache-hit', 'hit');
        const enabled = new Function('github', 'hit', 'return (' + expression + ');');
        for (const [event, ref] of [
          ['pull_request', 'refs/pull/123/merge'],
          ['merge_group', 'refs/heads/gh-readonly-queue/main/pr-123'],
          ['workflow_dispatch', 'refs/heads/feature'],
        ]) assert.equal(enabled({ event_name: event, ref }, 'false'), false, name + '/' + job);
        assert.equal(enabled({ event_name: 'schedule', ref: 'refs/heads/main' }, 'false'),
          condition !== 'false', name + '/' + job);
        if (step.includes('uses: actions/cache/save@')) {
          assert.equal(enabled({ ref: 'refs/heads/main' }, 'true'), false);
        }
        checked++;
      }
    }
  }
  assert.ok(checked >= 10);
});

test('helper tool versions do not fragment unrelated Rust caches', () => {
  for (const { text } of workflows) {
    assert.doesNotMatch(text, /CARGO_(SHEAR|FUZZ)_VERSION/);
  }
  const integration = workflows.find(w => w.name === 'integration').text;
  assert.match(integration, /prefix-key: "v1-fuzz-\$\{\{ env.FUZZ_VERSION \}\}"/);
  assert.match(integration, /cargo install cargo-fuzz --version "\$\{FUZZ_VERSION\}"/);
});

// Evaluate the actual job guard as well as the cache's save condition. A valid
// save-if alone cannot prove that a scheduled cache producer is reachable.
function warmupJob(workflow, job, {
  event = 'schedule', ref = 'refs/heads/main',
  profile = event === 'workflow_dispatch' ? 'all' : undefined,
  outputs = {}, failed = '', cancelled = false, draft = false,
} = {}) {
  const text = workflows.find(w => w.name === workflow).text;
  assert.match(text, /^  schedule:$/m);
  const body = jobs(text).find(([, name]) => name === job)?.[2];
  assert.ok(body, workflow + '/' + job);
  const github = { repository: 'GreptimeTeam/greptimedb', event_name: event, ref,
    event: { pull_request: { draft } }, run_attempt: 1 };
  const needs = { scope: { outputs }, ...Object.fromEntries(
    ['build', 'prepare-fuzz-targets', 'build-greptime-ci'].map(name =>
      [name, { result: name === failed ? 'failure' : 'success' }])) };
  const evaluate = expression => new Function('github', 'inputs', 'needs', 'cancelled',
    'return (' + expression.replace(/^\$\{\{\s*|\s*\}\}$/g, '') + ');')(
    github, { fuzz_profile: profile, ci_command: false }, needs, () => cancelled);
  const admitted = evaluate(body.match(/^    if: (.+)$/m)[1]);
  const saves = [...body.matchAll(/^          save-if: (.+)$/gm)]
    .map(match => admitted && evaluate(match[1]));
  return { admitted, saves };
}

const fuzzJobs = ['prepare-fuzz-targets', 'build-greptime-ci', 'fuzztest',
  'unstable-fuzztest', 'distributed-fuzztest', 'distributed-fuzztest-with-chaos'];

test('main schedules can execute cache producers and their fuzz consumers', () => {
  for (const job of fuzzJobs) {
    // Scheduled events do not supply workflow_dispatch inputs.
    const result = warmupJob('integration', job, { profile: undefined });
    assert.equal(result.admitted, true, job);
    assert.ok(result.saves.every(Boolean), job);
  }
  for (const [workflow, job] of [
    ['integration', 'build'], ['integration', 'export-import-v2-e2e'],
    ['integration', 'prepare-fuzz-targets'], ['integration', 'build-greptime-ci'],
    ['integration', 'distributed-fuzztest'],
    ['rust', 'check-windows'], ['rust', 'check-riscv64'],
    ['nightly-ci', 'sqlness-windows'], ['nightly-ci', 'test-on-windows'],
  ]) {
    for (const event of ['schedule', 'workflow_dispatch']) {
      assert.deepEqual(warmupJob(workflow, job, { event }),
        { admitted: true, saves: [true] }, workflow + '/' + job + '/' + event);
    }
  }
});

test('manual warming respects selected profiles without an internal command marker', () => {
  const expected = {
    none: [],
    standalone: fuzzJobs.slice(0, 4),
    distributed: [fuzzJobs[0], fuzzJobs[1], fuzzJobs[4]],
    chaos: [fuzzJobs[0], fuzzJobs[1], fuzzJobs[5]],
    all: fuzzJobs,
  };
  for (const [profile, wanted] of Object.entries(expected)) {
    assert.deepEqual(fuzzJobs.filter(job => warmupJob('integration', job,
      { event: 'workflow_dispatch', profile }).admitted), wanted, profile);
  }
  for (const job of fuzzJobs) {
    const result = warmupJob('integration', job,
      { event: 'workflow_dispatch', ref: 'refs/heads/feature' });
    assert.ok(result.saves.every(value => value === false), job);
    assert.equal(warmupJob('integration', job,
      { event: 'pull_request', ref: 'refs/pull/1/merge', draft: true }).admitted, false, job);
  }
});

test('warmup keeps scope exclusions, dependency failures and cancellation gates', () => {
  const gates = ['any_fuzz', 'build_ci_needed', 'fuzz_standalone',
    'fuzz_unstable', 'fuzz_distributed', 'fuzz_chaos'];
  for (const [index, job] of fuzzJobs.entries()) {
    assert.equal(warmupJob('integration', job, { cancelled: true }).admitted, false, job);
    assert.equal(warmupJob('integration', job,
      { outputs: { [gates[index]]: 'false' } }).admitted, false, job);
  }
  for (const job of fuzzJobs.slice(2)) {
    for (const failed of ['prepare-fuzz-targets', job === 'fuzztest' ? 'build' : 'build-greptime-ci']) {
      assert.equal(warmupJob('integration', job, { failed }).admitted, false, job);
    }
  }
});
