/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {getOctokit} from '@actions/github';
import type {PullRequestEditedEvent, PullRequestOpenedEvent} from '@octokit/webhooks-types';
import assert from 'node:assert/strict';
import test from 'node:test';
import {syncDocsLabel} from '../bin/follow-up-docs-issue';

test('Docbot maintains only docs-required and preserves manual overrides on unrelated edits', async t => {
  const client = getOctokit('test');
  const unchecked = '- [ ] This PR requires documentation updates.';
  const checked = '- [X] This PR requires documentation updates. See the docs PR.';
  let current = {body: unchecked, labels: [{name: 'docs-required'}]};
  const get = t.mock.method(client.rest.pulls, 'get', async () => ({data: current}));
  const writes: string[] = [];
  t.mock.method(client.rest.issues, 'addLabels', async ({labels}) => writes.push(`add ${labels.join(', ')}`));
  t.mock.method(client.rest.issues, 'removeLabel', async ({name}) => writes.push(`remove ${name}`));

  const pr = {number: 1, body: unchecked};
  const open = {action: 'opened', pull_request: pr} as PullRequestOpenedEvent;
  const edit = {action: 'edited', pull_request: pr, changes: {}} as PullRequestEditedEvent;
  await syncDocsLabel(client, 'owner', 'repo', open);
  await syncDocsLabel(client, 'owner', 'repo', edit);
  edit.changes.body = {from: unchecked + '\nOld nightly checkbox'};
  await syncDocsLabel(client, 'owner', 'repo', edit);
  assert.equal(get.mock.callCount(), 0);

  edit.changes.body = {from: checked};
  await syncDocsLabel(client, 'owner', 'repo', edit);
  current = {body: checked, labels: []};
  await syncDocsLabel(client, 'owner', 'repo', edit);
  assert.deepEqual(writes, ['remove docs-required', 'add docs-required']);
});
