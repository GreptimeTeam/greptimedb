import * as core from "@actions/core";
import {obtainClient} from "@/common";

async function triggerWorkflow(workflowId: string, version: string) {
  const docsClient = obtainClient("DOCS_REPO_TOKEN")
  try {
    await docsClient.rest.actions.createWorkflowDispatch({
      owner: "GreptimeTeam",
      repo: "docs",
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

const version = process.env.VERSION;
if (!version) {
  core.setFailed("VERSION environment variable is required");
  process.exit(1);
}

// Remove 'v' prefix if exists
const cleanVersion = version.startsWith('v') ? version.slice(1) : version;

try {
  const [workflowId, apiVersion] = determineWorkflow(cleanVersion);
  triggerWorkflow(workflowId, apiVersion);
} catch (error) {
  core.setFailed(`Error processing version: ${error.message}`);
  process.exit(1);
}
