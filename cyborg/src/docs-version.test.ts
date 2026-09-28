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

import assert from 'node:assert/strict';
import test from 'node:test';

import {docsWorkflowDispatch} from './docs-version';

test('cuts a missing documentation line for stable and prerelease versions', () => {
  for (const version of ['1.3.0', '1.3.0-alpha.1', '1.3.0-beta.2', '1.3.0-rc.1', '1.3.0-alpha.1+build.7']) {
    assert.deepEqual(docsWorkflowDispatch(version, ['1.2']), {
      workflowId: 'bump-version.yml',
      inputs: {version: '1.3'},
    });
  }
});

test('bumps the exact version when its documentation line exists', () => {
  assert.deepEqual(docsWorkflowDispatch('1.3.0-beta.2', ['1.3']), {
    workflowId: 'bump-patch-version.yml',
    inputs: {version: '1.3.0-beta.2'},
  });
});

test('keeps nightly versions on the nightly workflow', () => {
  assert.deepEqual(docsWorkflowDispatch('1.3.0-nightly-20260903', []), {
    workflowId: 'bump-nightly-version.yml',
    inputs: {version: '1.3.0-nightly-20260903'},
  });
});

test('rejects malformed non-nightly versions', () => {
  for (const version of ['1.3', 'v1.3.0', '1.3.0-', '1.3.0-alpha..1']) {
    assert.throws(() => docsWorkflowDispatch(version, []), /Invalid version format/);
  }
});
