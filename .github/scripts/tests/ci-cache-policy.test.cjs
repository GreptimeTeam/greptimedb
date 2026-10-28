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
