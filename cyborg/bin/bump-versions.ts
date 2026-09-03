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
import {docsWorkflowDispatch, WorkflowDispatch} from "@/docs-version";

interface RepoConfig {
  tokenEnv: string;
  repo: string;
  workflowLogic: (version: string) => WorkflowDispatch | null | Promise<WorkflowDispatch | null>;
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
      return {
        workflowId: 'bump-patch-version.yml',
        inputs: {version},
      };
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
      return {
        workflowId: 'bump-patch-version.yml',
        inputs: {version},
      };
    }
  },
  docs: {
    tokenEnv: "DOCS_REPO_TOKEN",
    repo: "docs",
    workflowLogic: async (version: string): Promise<WorkflowDispatch | null> => {
      // Nightly versions do not need the current docs version list.
      if (version.includes('nightly')) {
        return docsWorkflowDispatch(version, []);
      }

      const client = obtainClient('DOCS_REPO_TOKEN');
      const {data} = await client.rest.repos.getContent({
        owner: 'GreptimeTeam',
        repo: 'docs',
        path: 'versions.json',
        ref: 'main',
      });
      if (Array.isArray(data) || data.type !== 'file') {
        throw new Error('Expected docs versions.json to be a file');
      }

      const versions = JSON.parse(Buffer.from(data.content, 'base64').toString('utf-8'));
      if (!Array.isArray(versions) || !versions.every((entry) => typeof entry === 'string')) {
        throw new Error('Expected docs versions.json to be a string array');
      }

      return docsWorkflowDispatch(version, versions);
    }
  }
};

async function triggerWorkflow(repoConfig: RepoConfig, dispatch: WorkflowDispatch) {
  const client = obtainClient(repoConfig.tokenEnv);
  try {
    await client.rest.actions.createWorkflowDispatch({
      owner: "GreptimeTeam",
      repo: repoConfig.repo,
      workflow_id: dispatch.workflowId,
      ref: "main",
      inputs: dispatch.inputs,
    });
    console.log(`Successfully triggered ${dispatch.workflowId} workflow for ${repoConfig.repo} with version ${dispatch.inputs.version}`);
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
    const dispatch = await repoConfig.workflowLogic(version);
    if (dispatch === null) {
      // Skip this repo (e.g., nightly version for website)
      return;
    }

    await triggerWorkflow(repoConfig, dispatch);
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
