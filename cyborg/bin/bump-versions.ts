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

import * as core from "@actions/core";
import {obtainClient} from "@/common";

interface RepoConfig {
  tokenEnv: string;
  repo: string;
  workflowLogic: (version: string) => [string, string] | null;
}

const REPO_CONFIGS: Record<string, RepoConfig> = {
  website: {
    tokenEnv: "WEBSITE_REPO_TOKEN",
    repo: "website",
    workflowLogic: (version: string) => {
      // Skip nightly versions for website
      if (version.includes('nightly')) {
        console.log('Nightly version detected for website, skipping workflow trigger.');
        return null;
      }
      return ['bump-patch-version.yml', version];
    }
  },
  demo: {
    tokenEnv: "DEMO_REPO_TOKEN",
    repo: "demo-scene",
    workflowLogic: (version: string) => {
      // Skip nightly versions for demo
      if (version.includes('nightly')) {
        console.log('Nightly version detected for demo, skipping workflow trigger.');
        return null;
      }
      return ['bump-patch-version.yml', version];
    }
  },
  docs: {
    tokenEnv: "DOCS_REPO_TOKEN",
    repo: "docs",
    workflowLogic: (version: string) => {
      // Check if it's a nightly version
      if (version.includes('nightly')) {
        return ['bump-nightly-version.yml', version];
      }

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
  }
};

async function triggerWorkflow(repoConfig: RepoConfig, workflowId: string, version: string) {
  const client = obtainClient(repoConfig.tokenEnv);
  try {
    await client.rest.actions.createWorkflowDispatch({
      owner: "GreptimeTeam",
      repo: repoConfig.repo,
      workflow_id: workflowId,
      ref: "main",
      inputs: {
        version,
      },
    });
    console.log(`Successfully triggered ${workflowId} workflow for ${repoConfig.repo} with version ${version}`);
  } catch (error) {
    core.setFailed(`Failed to trigger workflow for ${repoConfig.repo}: ${error.message}`);
    throw error;
  }
}

async function processRepo(repoName: string, version: string) {
  const repoConfig = REPO_CONFIGS[repoName];
  if (!repoConfig) {
    throw new Error(`Unknown repository: ${repoName}`);
  }

  try {
    const workflowResult = repoConfig.workflowLogic(version);
    if (workflowResult === null) {
      // Skip this repo (e.g., nightly version for website)
      return;
    }

    const [workflowId, apiVersion] = workflowResult;
    await triggerWorkflow(repoConfig, workflowId, apiVersion);
  } catch (error) {
    core.setFailed(`Error processing ${repoName} with version ${version}: ${error.message}`);
    throw error;
  }
}

async function main() {
  const version = process.env.VERSION;
  if (!version) {
    core.setFailed("VERSION environment variable is required");
    process.exit(1);
  }

  // Remove 'v' prefix if exists
  const cleanVersion = version.startsWith('v') ? version.slice(1) : version;

  // Get target repositories from environment variable
  // Default to both if not specified
  const targetRepos = process.env.TARGET_REPOS?.split(',').map(repo => repo.trim()) || ['website', 'docs'];

  console.log(`Processing version ${cleanVersion} for repositories: ${targetRepos.join(', ')}`);

  const errors: string[] = [];

  // Process each repository
  for (const repo of targetRepos) {
    try {
      await processRepo(repo, cleanVersion);
    } catch (error) {
      errors.push(`${repo}: ${error.message}`);
    }
  }

  if (errors.length > 0) {
    core.setFailed(`Failed to process some repositories: ${errors.join('; ')}`);
    process.exit(1);
  }

  console.log('All repositories processed successfully');
}

// Execute main function
main().catch((error) => {
  core.setFailed(`Unexpected error: ${error.message}`);
  process.exit(1);
});
