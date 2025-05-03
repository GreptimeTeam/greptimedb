/*
 * Copyright 2025 Greptime Team
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

import * as core from "@actions/core";
import {obtainClient} from "@/common";

async function triggerWorkflow(workflowId: string, version: string) {
  const websiteClient = obtainClient("WEBSITE_REPO_TOKEN")
  try {
    await websiteClient.rest.actions.createWorkflowDispatch({
      owner: "GreptimeTeam",
      repo: "website",
      workflow_id: workflowId,
      ref: "main",
      inputs: {
        version,
      },
    });
    console.log(`Successfully triggered ${workflowId} workflow with version ${version}`);
  } catch (error) {
    core.setFailed(`Failed to trigger workflow: ${error.message}`);
  }
}

function determineWorkflow(version: string): [string, string] {
  const parts = version.split('.');

  if (parts.length !== 3) {
    throw new Error('Invalid version format');
  }

  // If patch version (last number) is 0, it's a major version
  // Return only major.minor version
  if (parts[2] === '0') {
    return ['bump-version.yml', `${parts[0]}.${parts[1]}`];
  }

  // Otherwise it's a patch version, use full version
  return ['bump-patch-version.yml', version];
}

const version = process.env.VERSION;
if (!version) {
  core.setFailed("VERSION environment variable is required");
  process.exit(1);
}

// Remove 'v' prefix if exists
const cleanVersion = version.startsWith('v') ? version.slice(1) : version;

if (cleanVersion.includes('nightly')) {
  console.log('Nightly version detected, skipping workflow trigger.');
  process.exit(0);
}

try {
  const [workflowId, apiVersion] = determineWorkflow(cleanVersion);
  triggerWorkflow(workflowId, apiVersion);
} catch (error) {
  core.setFailed(`Error processing version: ${error.message}`);
  process.exit(1);
}
