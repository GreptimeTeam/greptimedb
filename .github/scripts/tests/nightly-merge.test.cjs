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

// Run with: node --test .github/scripts/tests/nightly-merge.test.cjs
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');
const assert = require('node:assert/strict');
const { test } = require('node:test');
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const workflow = readFileSync(resolve(__dirname, '../../workflows/nightly-merge.yml'), 'utf8');
const scripts = [...workflow.matchAll(/          script: \|\n((?:            .*\n|\n)+)/g)]
  .map(match => new AsyncFunction('github', 'context', 'core', 'process', match[1].replace(/^            /gm, '')));
assert.equal(scripts.length, 2);
const checked = '- [x] Merge during the nightly window <!-- nightly-merge -->';

async function run({ sync = false, body = checked, labels = ['merge nightly'],
  detail = {}, current = {}, threads = [[true]], count = 1, dryRun = false,
  failFirst = false } = {}) {
  const calls = [];
  let page = 0;
  const github = {
    rest: {
      pulls: { get: async () => ({ data: { state: 'open', draft: false, base: { ref: 'main' },
        body, labels: labels.map(name => ({ name })), ...current } }) },
      issues: {
        listForRepo: () => {},
        addLabels: async args => calls.push(['add', args.labels]),
        removeLabel: async args => calls.push(['remove', args.name])
      }
    },
    paginate: async () => Array.from({ length: count }, (_, i) => ({ number: i + 1, pull_request: {} })),
    graphql: async (query, args) => {
      if (query.includes('mutation')) {
        calls.push(['enqueue', args]);
        if (failFirst && calls.filter(call => call[0] === 'enqueue').length === 1) throw Error('head changed');
        return {};
      }
      if (query.includes('reviewThreads')) {
        const index = page++ % threads.length;
        calls.push(['threads', args.cursor]);
        return { repository: { pullRequest: { reviewThreads: {
          nodes: threads[index].map(isResolved => ({ isResolved })),
          pageInfo: { hasNextPage: index + 1 < threads.length, endCursor: `page-${index}` }
        } } } };
      }
      return { repository: { pullRequest: { id: `PR_${args.number}`, headRefOid: 'abc',
        reviewDecision: 'APPROVED', mergeStateStatus: 'CLEAN', ...detail } } };
    }
  };
  const summary = { addHeading() { return this; }, addTable(rows) { calls.push(['summary', rows]); return this; }, async write() {} };
  await scripts[sync ? 0 : 1](github,
    { repo: { owner: 'GreptimeTeam', repo: 'greptimedb' }, payload: { pull_request: { number: 1 } } },
    { summary, warning() {}, setFailed: message => calls.push(['failed', message]) },
    { env: { DRY_RUN: String(dryRun) } });
  return calls;
}

test('template opts in and checkbox edits reconcile only the nightly label', async () => {
  assert.ok(readFileSync(resolve(__dirname, '../../pull_request_template.md'), 'utf8').includes(checked));
  assert.deepEqual(await run({ sync: true, labels: ['bug'] }), [['add', ['merge nightly']]]);
  assert.deepEqual(await run({ sync: true }), []);
  for (const body of [checked.replace('[x]', '[ ]')]) {
    assert.deepEqual(await run({ sync: true, body }), [['remove', 'merge nightly']]);
  }
  assert.deepEqual(await run({ sync: true, body: checked.replace('[x]', '[X]'), labels: [] }), [['add', ['merge nightly']]]);
});

test('eligible PR is enqueued with its checked head; dry run performs no mutation', async () => {
  assert.deepEqual((await run()).filter(c => c[0] === 'enqueue'), [['enqueue', { id: 'PR_1', sha: 'abc' }]]);
  assert.equal((await run({ dryRun: true })).some(c => c[0] === 'enqueue'), false);
});

test('each eligibility gate independently prevents enqueueing', async () => {
  for (const options of [
    { labels: [] },
    { current: { draft: true } }, { current: { state: 'closed' } }, { current: { base: { ref: 'release/v1.3' } } },
    { detail: { reviewDecision: 'REVIEW_REQUIRED' } }, { detail: { reviewDecision: 'CHANGES_REQUESTED' } },
    { detail: { mergeStateStatus: 'BLOCKED' } }, { detail: { mergeStateStatus: 'UNKNOWN' } },
    { detail: { mergeQueueEntry: { id: 'entry' } } }, { detail: { autoMergeRequest: { enabledAt: 'now' } } },
    { threads: [[false]] }, { threads: [[true], [false]] }
  ]) {
    assert.equal((await run(options)).some(c => c[0] === 'enqueue'), false, JSON.stringify(options));
  }
});

test('all thread pages are checked before enqueueing', async () => {
  const calls = await run({ threads: [[true], [true]] });
  assert.deepEqual(calls.filter(c => c[0] === 'threads'), [['threads', null], ['threads', 'page-0']]);
  assert.equal(calls.filter(c => c[0] === 'enqueue').length, 1);
});

test('per-run cap and individual failures do not prevent later candidates', async () => {
  assert.equal((await run({ count: 8 })).filter(c => c[0] === 'enqueue').length, 5);
  const calls = await run({ count: 2, failFirst: true });
  assert.equal(calls.filter(c => c[0] === 'enqueue').length, 2);
  assert.equal(calls.filter(c => c[0] === 'failed').length, 1);
});

test('label-only opt-in survives PRs without a template and does not require a checkbox', async () => {
  for (const body of ['', null, 'Existing PR description', `> ${checked}`]) {
    assert.deepEqual(await run({ sync: true, body }), []);
    assert.deepEqual(await run({ sync: true, body, labels: [] }), []);
    assert.equal((await run({ body })).filter(call => call[0] === 'enqueue').length, 1);
    assert.equal((await run({ body, labels: [] })).some(call => call[0] === 'enqueue'), false);
  }
  // The label is authoritative for admission, even before checkbox synchronization.
  assert.equal((await run({ body: checked.replace('[x]', '[ ]') })).some(call => call[0] === 'enqueue'), true);
});
