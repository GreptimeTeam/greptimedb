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
const template = readFileSync(resolve(__dirname, '../../pull_request_template.md'), 'utf8');
const scripts = [...workflow.matchAll(/          script: \|\n((?:            .*\n|\n)+)/g)]
  .map(match => new AsyncFunction('github', 'context', 'core', 'process', 'require', match[1].replace(/^            /gm, '')));
assert.equal(scripts.length, 2);
const { changesFor } = require('../migrate-pr-labels.cjs');
const label = 'skip nightly merge';
const checked = '- [x] Skip nightly merge';
const unchecked = '- [ ] Skip nightly merge';
const legacy = '- [x] Merge during the nightly window <!-- nightly-merge -->';

function migrateMainPr(optedIn, pr) {
  return changesFor({ base: 'main', optedIn }, { ...pr, base: { ref: 'main' } });
}

async function run({ sync = false, body = unchecked, labels = [], events = [], payload = {},
  detail = {}, listed = {}, current = {}, threads = [[true]], count = 1, dryRun = false,
  failFirst = false, enqueueError = Error('head changed') } = {}) {
  const calls = [];
  let page = 0;
  const pr = { state: 'open', draft: false, base: { ref: 'main' }, body, labels: labels.map(name => ({ name })) };
  const github = {
    rest: {
      pulls: { list() {}, get: async () => {
        if (!sync) calls.push(['get']);
        return { data: { ...pr, ...current } };
      } },
      issues: {
        listEvents() {},
        addLabels: async args => calls.push(['add', args.labels]),
        removeLabel: async args => calls.push(['remove', args.name])
      }
    },
    paginate: async (route, args) => {
      if (route === github.rest.issues.listEvents) {
        assert.equal(args.issue_number, 1);
        return events;
      }
      assert.equal(route, github.rest.pulls.list);
      assert.deepEqual(args, { owner: 'GreptimeTeam', repo: 'greptimedb', state: 'open', base: 'main',
        sort: 'created', direction: 'asc', per_page: 100 });
      return Array.from({ length: count }, (_, i) => ({ ...pr, number: i + 1, ...listed }));
    },
    graphql: async (query, args) => {
      if (query.includes('mutation')) {
        calls.push(['enqueue', args]);
        if (failFirst && calls.filter(call => call[0] === 'enqueue').length === 1) throw enqueueError;
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
    { repo: { owner: 'GreptimeTeam', repo: 'greptimedb' }, payload: { pull_request: { number: 1 }, ...payload } },
    { summary, info() {}, warning() {}, setFailed: message => calls.push(['failed', message]) },
    { env: { DRY_RUN: String(dryRun) } }, path => require(resolve(__dirname, '../../..', path)));
  return calls;
}

test('label sync reads the helper from the trusted workflow commit', () => {
  const syncJob = workflow.split('  sync-label:\n')[1].split('  enqueue:\n')[0];
  assert.match(syncJob, /^      pull-requests: write$/m);
  assert.match(syncJob, /ref: \$\{\{ github\.sha \}\}/);
  assert.match(syncJob, /persist-credentials: false/);
  assert.doesNotMatch(syncJob, /pull_request\.head/);
});

test('removing the skip label reconciles the current checkbox on main', async () => {
  const trigger = workflow.split('  pull_request_target:\n')[1].split('  schedule:\n')[0];
  assert.match(trigger, /branches: \[main\]/);
  assert.match(trigger, /types: \[[^\]\n]*\bunlabeled\b/);
  const guard = workflow.split('  sync-label:\n')[1].match(/    if: >-\n((?:      .*\n)+)/)[1];
  const enabled = new Function('github', `return (${guard});`);
  const payload = { action: 'unlabeled', label: { name: label } };
  const admits = event => enabled({ repository: 'GreptimeTeam/greptimedb', event_name: 'pull_request_target', event });
  assert.equal(admits(payload), true);
  assert.equal(admits({ ...payload, label: { name: 'docs-required' } }), false);
  assert.equal(admits({ action: 'edited' }), true);

  const opts = { sync: true, payload };
  assert.deepEqual(await run({ ...opts, body: checked }), [['add', [label]]]);
  for (const body of [unchecked, null]) assert.deepEqual(await run({ ...opts, body }), []);
  assert.deepEqual(await run({ ...opts, body: checked, current: { base: { ref: 'release/v1.3' } } }), []);
});

test('enqueueing requires rollout activation and dry runs work while paused', () => {
  const guard = workflow.split('  enqueue:\n')[1].match(/    if: >-\n((?:      .*\n)+)/)[1];
  const enabled = new Function('github', 'vars', 'inputs', `return (${guard});`);
  const repo = { repository: 'GreptimeTeam/greptimedb' };
  for (const flag of ['', 'false', 'true']) {
    const vars = { NIGHTLY_MERGE_ENABLED: flag };
    const scheduled = { ...repo, event_name: 'schedule' };
    const manual = { ...repo, event_name: 'workflow_dispatch' };
    assert.equal(enabled(scheduled, vars, {}), flag === 'true');
    assert.equal(enabled(manual, vars, { dry_run: false }), flag === 'true');
    assert.equal(enabled(manual, vars, { dry_run: true }), true);
    assert.equal(enabled({ ...repo, event_name: 'pull_request_target' }, vars, {}), false);
  }
});

test('default template needs no label and a checked opt-out is effective before sync', async () => {
  assert.ok(template.includes(unchecked));
  assert.deepEqual(await run({ sync: true, body: template }), []);
  for (const body of [checked, checked.replace('[x]', '[X]') + ' — wait for backport',
    checked + ' <!-- note --> more details', unchecked + '\r\n' + checked]) {
    assert.deepEqual(await run({ sync: true, body, labels: ['bug'] }), [['add', [label]]]);
    assert.equal((await run({ body })).some(c => c[0] === 'enqueue'), false);
  }
  assert.deepEqual(await run({ sync: true }), []);
  assert.deepEqual(await run({ sync: true, body: checked, labels: [label] }), []);
});

test('a coalesced uncheck releases a bot hold; delayed edits and reopens preserve a newer manual hold', async () => {
  const bot = { id: 1, created_at: '2026-01-01T00:00:00Z', event: 'labeled', label: { name: label },
    actor: { login: 'github-actions[bot]' } };
  const removed = { ...bot, id: 2, event: 'unlabeled' };
  const manual = { ...bot, id: 3, actor: { login: 'maintainer' } };
  for (const payload of [{ action: 'edited', changes: { title: { from: 'before' } } },
    { action: 'edited', changes: { body: { from: checked } } }, { action: 'reopened' }]) {
    const opts = { sync: true, labels: [label], payload };
    assert.deepEqual(await run({ ...opts, events: [bot] }), [['remove', label]]);
    assert.deepEqual(await run({ ...opts, events: [manual, bot, removed] }), []);
    assert.deepEqual(await run({ ...opts, events: [] }), []);
  }
});

test('eligible PR is enqueued with its checked head; dry run performs no mutation', async () => {
  assert.deepEqual((await run()).filter(c => c[0] === 'enqueue'), [['enqueue', { id: 'PR_1', sha: 'abc' }]]);
  assert.equal((await run({ dryRun: true })).some(c => c[0] === 'enqueue'), false);
});

test('each eligibility gate independently prevents enqueueing', async () => {
  for (const options of [
    { labels: [label] }, { listed: { draft: true } }, { current: { labels: [{ name: label }] } },
    { current: { body: checked } },
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
  assert.ok(calls.findIndex(c => c[0] === 'get') > calls.findLastIndex(c => c[0] === 'threads'));
  assert.equal(calls.filter(c => c[0] === 'enqueue').length, 1);
});

test('per-run cap and individual failures do not prevent later candidates', async () => {
  assert.equal((await run({ count: 8 })).filter(c => c[0] === 'enqueue').length, 5);
  const calls = await run({ count: 2, failFirst: true });
  assert.equal(calls.filter(c => c[0] === 'enqueue').length, 2);
  assert.equal(calls.filter(c => c[0] === 'failed').length, 1);
});

test('checkbox-free PRs enter by default and manual holds work on any description', async () => {
  for (const body of ['', null, 'Automated PR', `> ${checked}`, `\`\`\`md\n${checked}\n\`\`\``,
    `<!--\n${checked}\n-->`, legacy, legacy.replace('[x]', '[ ]')]) {
    assert.deepEqual(await run({ sync: true, body }), []);
    assert.equal((await run({ body })).filter(call => call[0] === 'enqueue').length, 1);
    assert.equal((await run({ body, labels: [label] })).some(call => call[0] === 'enqueue'), false);
  }
});

test('list fences hide example checkboxes and leave the following merge checklist effective', async () => {
  for (const [opening, indent, closing] of [
    ['1. ```sql', '   ', '   ```'],
    ['10) ~~~md', '    ', '      ~~~~'],
    ['- ```md', '  ', ''],
    ['1.\t```md', '    ', '\t```'],
    ['1. Reproduction\n   ```md', '   ', '']
  ]) {
    const description = (sample, box) =>
      [opening, indent + 'SELECT 1;', '', indent + sample, closing, '', '## Merge timing', '', box].join('\n');
    for (const [sample, box] of [[unchecked, checked], [checked, unchecked]]) {
      const body = description(sample, box);
      assert.deepEqual(await run({ sync: true, body }), box === checked ? [['add', [label]]] : [], body);
      assert.equal((await run({ body })).some(call => call[0] === 'enqueue'), box === unchecked, body);
    }
    assert.equal(migrateMainPr(false, { body: description(legacy, legacy), labels: [] }).body,
      description(legacy, checked));
  }
});

test('migration follows the snapshotted label, edits only visible legacy items and preserves docs overrides', () => {
  for (const optedIn of [false, true]) {
    const old = '- [ ] Merge during the nightly window';
    const before = ['Author notes', `> ${old}`, '```md', old, '```', '<!--', old, '-->', ''];
    const pr = { body: [...before, old + ' — note', legacy + ' (later)'].join('\r\n'),
      labels: ['docs-required', 'bug', 'docs-not-required', ...(optedIn ? ['merge nightly'] : [])].map(name => ({ name })) };
    const result = migrateMainPr(optedIn, pr);
    const replacement = optedIn ? unchecked : checked;
    assert.equal(result.body, [...before, replacement + ' — note', replacement + ' (later)'].join('\r\n'));
    assert.deepEqual(result.remove, optedIn ? ['merge nightly', 'docs-not-required'] : ['docs-not-required']);
    assert.equal(result.addHold, false);
    assert.equal(result.waitingForSync, !optedIn);
    const migrated = { body: result.body, labels: optedIn ? [] : [{ name: label }] };
    assert.deepEqual(migrateMainPr(optedIn, migrated), { body: migrated.body, addHold: false, remove: [],
      waitingForSync: false, extraHold: false, needsChoice: false });
  }
});

test('retrying migration honors a cleared nightly opt-out checkbox', () => {
  const retry = migrateMainPr(false, { body: unchecked, labels: [] });
  assert.deepEqual(retry, { body: unchecked, addHold: false, remove: [],
    waitingForSync: false, extraHold: false, needsChoice: false });
});

test('migration only cleans the old docs label on release PRs, including PRs retargeted from main', () => {
  const release = 'release/v1.3';
  for (const [base, optedIn] of [[release, true], ['main', false]]) {
    const names = ['docs-required', 'docs-not-required', ...(optedIn ? ['merge nightly'] : [])];
    for (const body of [legacy, null, checked]) {
      const pr = { base: { ref: release }, body, labels: names.map(name => ({ name })) };
      assert.deepEqual(changesFor({ base, optedIn }, pr), { body, addHold: false, remove: ['docs-not-required'],
        waitingForSync: false, extraHold: false, needsChoice: false });
    }
  }
});

test('a PR retargeted into main needs a current checkbox or manual hold', () => {
  const entry = { base: 'release/v1.3', optedIn: true };
  for (const [body, names, needsChoice, waitingForSync] of [
    [legacy, ['merge nightly'], true, false], [null, [], true, false],
    ['> ' + unchecked, [], true, false], [unchecked, [], false, false],
    [checked, [], false, true], [null, [label], false, false]
  ]) {
    const pr = { base: { ref: 'main' }, body, labels: names.map(name => ({ name })) };
    assert.deepEqual(changesFor(entry, pr), { body, addHold: false, remove: [],
      waitingForSync, extraHold: false, needsChoice });
  }
});

test('migration updates the stock help only in the comment following a converted checkbox', () => {
  const oldHelp = 'Uncheck to opt out and remove the `merge nightly` label; this does not enable immediate merging.';
  const newHelp = 'Check to opt out. To resume, uncheck the box and remove any manually applied `skip nightly merge` label.';
  const migrate = (body, optedIn) => migrateMainPr(optedIn, { body, labels: [] }).body;
  assert.ok(template.includes(newHelp));
  assert.equal(migrate(template.replace(unchecked, legacy).replace(newHelp, oldHelp), true), template);

  const description = (box, help) => [
    oldHelp, '', '```md', legacy, '<!--', oldHelp, '-->', '```', '',
    box + ' — waiting for a backport', '', '<!--', 'Author note', help, '-->', '',
    '<!--', oldHelp, '-->'
  ].join('\r\n');
  assert.equal(migrate(description(legacy, oldHelp), false), description(checked, newHelp));
  assert.equal(migrate(description(legacy, oldHelp + ' Custom note.'), false),
    description(checked, oldHelp + ' Custom note.'));
  assert.equal(migrate(description(checked, oldHelp), false), description(checked, oldHelp));
});

test('migration holds checkbox-free PRs across retries and flags a later manual opt-out for review', () => {
  const pr = { body: null, labels: [] };
  assert.equal(migrateMainPr(false, pr).addHold, true);
  assert.equal(migrateMainPr(true, pr).addHold, false);
  pr.labels.push({ name: label });
  assert.equal(migrateMainPr(false, pr).addHold, false);
  assert.equal(migrateMainPr(true, pr).extraHold, true);
  assert.equal(migrateMainPr(false, pr).body, null);
});

const stackMessage = 'This pull request is part of a stack and must be enqueued using the asynchronous merge REST API. For more information, see https://docs.github.com/rest/pulls/pulls#merge-a-pull-request-asynchronously';

test('stacked PR rejection is skipped without consuming the successful enqueue cap', async () => {
  const calls = await run({ count: 7, failFirst: true,
    enqueueError: Object.assign(Error('GraphQL request failed'), { errors: [{ message: stackMessage }] }) });
  assert.equal(calls.some(c => c[0] === 'failed'), false);
  assert.deepEqual(calls.find(c => c[0] === 'summary')[1], [
    ['PR', 'Result'], ['1', 'Skipped: stacked PR'],
    ...[2, 3, 4, 5, 6].map(number => [String(number), 'Enqueued'])
  ]);
  assert.deepEqual(calls.filter(c => c[0] === 'enqueue').map(c => c[1].id),
    ['PR_1', 'PR_2', 'PR_3', 'PR_4', 'PR_5', 'PR_6']);
});

test('mixed or unstructured errors are not mistaken for a stacked PR rejection', async () => {
  for (const enqueueError of [
    Error(stackMessage),
    Object.assign(Error('GraphQL request failed'), { errors: [] }),
    Object.assign(Error('GraphQL request failed'), { errors: [{ message: stackMessage }, { message: 'Forbidden' }] })
  ]) {
    const calls = await run({ count: 2, failFirst: true, enqueueError });
    assert.equal(calls.filter(c => c[0] === 'failed').length, 1);
    assert.deepEqual(calls.find(c => c[0] === 'summary')[1], [
      ['PR', 'Result'], ['1', 'Failed: see workflow logs'], ['2', 'Enqueued']
    ]);
  }
});
