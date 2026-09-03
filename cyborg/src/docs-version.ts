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

export interface WorkflowDispatch {
  workflowId: string;
  inputs: Record<string, string>;
}

const DOCS_VERSION_RE = /^(\d+)\.(\d+)\.\d+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export function docsWorkflowDispatch(version: string, versions: string[]): WorkflowDispatch {
  if (version.includes('nightly')) {
    return {
      workflowId: 'bump-nightly-version.yml',
      inputs: {version},
    };
  }

  const match = version.match(DOCS_VERSION_RE);
  if (!match) {
    throw new Error('Invalid version format');
  }

  const docsVersion = `${match[1]}.${match[2]}`;
  if (versions.includes(docsVersion)) {
    return {
      workflowId: 'bump-patch-version.yml',
      inputs: {version},
    };
  }

  return {
    workflowId: 'bump-version.yml',
    inputs: {version: docsVersion},
  };
}
