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

// Trusted admission gate for the `Query Regression Controller` workflow.
//
// This script is default-branch trusted code. It runs in the controller
// workflow and decides, from GitHub API data only, whether the originating
// build may be turned into an ACK benchmark Job. There are two source modes
// with different originating-run shapes:
//
//   * SOURCE_MODE=workflow_run: the controller follows the unprivileged
//     `Query Regression` build workflow; the originating run must be a
//     COMPLETED, SUCCESSFUL `Query Regression` run at the exact workflow file
//     `.github/workflows/query-regression.yml` (pull_request label runs and
//     standalone workflow_dispatch builds).
//   * SOURCE_MODE=workflow_call: the controller is invoked synchronously by
//     release.yml, where the build and the controller are reusable-workflow
//     jobs in the SAME enclosing `Release` run. The originating run is that
//     enclosing run: while the controller executes it must still be IN
//     PROGRESS (status `in_progress`, null/empty conclusion), its name must
//     be exactly `Release` and its workflow file path (canonicalized: the
//     REST `path` field appends the triggering `@ref`, which is stripped)
//     exactly `.github/workflows/release.yml`, and its event one of
//     push|schedule|workflow_dispatch. Its run id/attempt must equal the
//     controller's own run id/attempt (both jobs share the enclosing run).
//
// It mirrors the validation patterns of `query-regression-comment.cjs`
// (run/PR/head/repo pinning, fork `pull_requests` fallback, stale/moved-PR
// rejection) and adds the controller-specific checks the comment workflow
// does not need:
//
//   * the originating run must be the exact expected workflow name and
//     workflow file path (per source mode), repository, run id, run attempt,
//     event, status/conclusion, and head SHA;
//   * the `query-regression-metadata` artifact (treated as untrusted, because
//     it is produced by the build workflow that executes candidate code) must
//     agree with the GitHub API on every field that can be cross-checked:
//     run id/attempt/repository/event/head SHA, artifact ids, and for PRs the
//     PR number, head/base repos, admission label, and the candidate merge
//     SHA parent relationship (exactly one head parent + the built base SHA);
//   * PR label admission: the current PR must still carry a regression label
//     (a maintainer-only action) matching the label recorded in the metadata;
//   * artifact ids recorded in the metadata must equal the ids GitHub lists
//     for the originating run (replay / wrong-artifact rejection).
//
// Only after every check passes does the workflow download the base/candidate
// artifacts by the validated ids and hand the validated values to the trusted
// controller. On any failure this script exits non-zero (fail closed) and the
// controller workflow stops before any ACK credential is used.

'use strict';

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

const FULL_SHA_RE = /^[0-9a-f]{40}$/i;
// Events the originating run may report. pull_request and workflow_dispatch
// are the workflow_run-followed paths; the workflow_call source mode instead
// validates the enclosing Release run, whose event must be one of
// push|schedule|workflow_dispatch (ALLOWED_CALLER_EVENTS).
const ALLOWED_EVENTS = new Set(['pull_request', 'workflow_dispatch', 'push', 'schedule', 'workflow_call']);
// The metadata artifact records the build run's own event (GITHUB_EVENT_NAME
// of the build run); it must equal the API-reported run event exactly.
const ALLOWED_METADATA_EVENTS = new Set(['pull_request', 'workflow_dispatch', 'push', 'schedule', 'workflow_call']);
const ALLOWED_LABELS = new Set(['query-regression', 'heavy-regression']);
const DEFAULT_SOURCE_WORKFLOW = 'Query Regression';
// Only the Release workflow may invoke the controller via workflow_call, and
// only on its real triggering events.
const ALLOWED_CALLER_WORKFLOWS = new Set(['Release']);
const ALLOWED_CALLER_EVENTS = new Set(['push', 'schedule', 'workflow_dispatch']);
// Exact workflow file identity per source mode. The REST `path` field of a
// workflow run appends the triggering ref (`.github/workflows/x.yml@refs/...`);
// canonicalRunPath() strips that first `@ref` suffix before comparing to these
// exact file paths. Display-name checks (run.name) are retained as defense in
// depth.
const SOURCE_WORKFLOW_PATH = '.github/workflows/query-regression.yml';
// Exact identity of the enclosing Release workflow in workflow_call mode:
// the source run's name AND canonical workflow file path must match both.
const RELEASE_WORKFLOW_PATH = '.github/workflows/release.yml';
// The only admissible release tag ref shape: refs/tags/vN.N.N (exact, no
// suffix, no other tag name).
const RELEASE_TAG_REF_RE = /^refs\/tags\/v\d+\.\d+\.\d+$/;
const ARTIFACT_NAMES = {
  metadata: 'query-regression-metadata',
  base: 'query-regression-base-binaries',
  candidate: 'query-regression-candidate-binaries',
};

function isFullSha(value) {
  return typeof value === 'string' && FULL_SHA_RE.test(value);
}

function isPositiveInt(value) {
  return Number.isInteger(value) && value > 0;
}

// GitHub's REST/workflow_run payload `path` appends the triggering ref
// (`.github/workflows/x.yml@refs/heads/main`). Canonicalize to the workflow
// file path alone by stripping the first `@ref` suffix; unqualified paths
// (no `@`) are returned unchanged.
function canonicalRunPath(runPath) {
  if (typeof runPath !== 'string') {
    return runPath;
  }
  const atIndex = runPath.indexOf('@');
  return atIndex === -1 ? runPath : runPath.slice(0, atIndex);
}

function apiHeaders(token) {
  return {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${token}`,
  };
}

// ---------------------------------------------------------------------------
// Minimal ZIP reader (no dependencies) used to read the metadata artifact.
// ---------------------------------------------------------------------------

const EOCD_SIG = 0x06054b50;
const CD_SIG = 0x02014b50;
const LOCAL_SIG = 0x04034b50;

function findEocd(buf) {
  // EOCD is at most 65557 bytes from the end (max comment length 65535).
  const start = Math.max(0, buf.length - 65557);
  for (let i = buf.length - 22; i >= start; i -= 1) {
    if (buf.readUInt32LE(i) === EOCD_SIG) {
      const entryCount = buf.readUInt16LE(i + 10);
      const cdOffset = buf.readUInt32LE(i + 16);
      if (cdOffset >= 0 && cdOffset < buf.length) {
        return { entryCount, cdOffset };
      }
    }
  }
  throw new Error('metadata artifact is not a valid zip (no end-of-central-directory record)');
}

function readCentralEntry(buf, offset) {
  const nameLen = buf.readUInt16LE(offset + 28);
  const extraLen = buf.readUInt16LE(offset + 30);
  const commentLen = buf.readUInt16LE(offset + 32);
  const localOffset = buf.readUInt32LE(offset + 42);
  const name = buf.toString('utf8', offset + 46, offset + 46 + nameLen);
  return {
    name,
    localOffset,
    next: offset + 46 + nameLen + extraLen + commentLen,
  };
}

function readLocalEntry(buf, localOffset, entryName) {
  const flags = buf.readUInt16LE(localOffset + 6);
  const method = buf.readUInt16LE(localOffset + 8);
  const compressedSize = buf.readUInt32LE(localOffset + 18);
  const nameLen = buf.readUInt16LE(localOffset + 26);
  const extraLen = buf.readUInt16LE(localOffset + 28);
  const dataStart = localOffset + 30 + nameLen + extraLen;
  const dataEnd = dataStart + compressedSize;
  if (dataEnd > buf.length) {
    throw new Error(`metadata artifact zip entry ${entryName} is truncated`);
  }
  const data = buf.subarray(dataStart, dataEnd);
  if ((flags & 0x08) !== 0) {
    // Data descriptor: compressed size in the local header is unreliable.
    throw new Error(`metadata artifact zip entry ${entryName} uses a data descriptor`);
  }
  if (method === 0) {
    return Buffer.from(data);
  }
  if (method === 8) {
    return zlib.inflateRawSync(data);
  }
  throw new Error(`metadata artifact zip entry ${entryName} uses unsupported compression ${method}`);
}

function unzipFirstEntry(buf, wantedName) {
  // Returns the decoded contents of the first zip entry whose name (after
  // stripping any leading directory components) matches `wantedName`.
  const { entryCount, cdOffset } = findEocd(buf);
  let offset = cdOffset;
  for (let i = 0; i < entryCount; i += 1) {
    if (buf.readUInt32LE(offset) !== CD_SIG) {
      throw new Error('metadata artifact zip has a corrupt central directory');
    }
    const entry = readCentralEntry(buf, offset);
    offset = entry.next;
    const baseName = path.posix.basename(entry.name.replace(/\\/g, '/'));
    if (baseName === wantedName) {
      return readLocalEntry(buf, entry.localOffset, entry.name);
    }
  }
  throw new Error(`metadata artifact zip does not contain ${wantedName}`);
}

// ---------------------------------------------------------------------------
// GitHub API client (injectable for tests)
// ---------------------------------------------------------------------------

class AdmissionError extends Error {}

function createValidator({ fetchImpl, log = () => {} }) {
  async function request(apiUrl, token, urlPath) {
    let res;
    try {
      res = await fetchImpl(`${apiUrl}${urlPath}`, {
        headers: apiHeaders(token),
        redirect: 'follow',
      });
    } catch (error) {
      throw new AdmissionError(`GitHub API request to ${urlPath} failed: ${error.message}`);
    }
    if (!res.ok) {
      throw new AdmissionError(`GitHub API ${urlPath} returned HTTP ${res.status}`);
    }
    return res;
  }

  async function getJson(apiUrl, token, urlPath) {
    const res = await request(apiUrl, token, urlPath);
    return res.json();
  }

  async function getBuffer(apiUrl, token, urlPath) {
    const res = await request(apiUrl, token, urlPath);
    return Buffer.from(await res.arrayBuffer());
  }

  async function listArtifacts(apiUrl, token, owner, repo, runId) {
    const artifacts = [];
    let page = 1;
    // The artifact list is small (three artifacts per run); paginate anyway so
    // a runaway artifact count cannot truncate the set we validate against.
    for (;;) {
      const data = await getJson(
        apiUrl, token,
        `/repos/${owner}/${repo}/actions/runs/${runId}/artifacts?per_page=100&page=${page}`
      );
      if (!Array.isArray(data.artifacts)) {
        throw new AdmissionError('artifact listing returned no artifacts array');
      }
      artifacts.push(...data.artifacts);
      if (data.artifacts.length < 100 || (Number.isInteger(data.total_count) && artifacts.length >= data.total_count)) {
        return artifacts;
      }
      page += 1;
    }
  }

  function findArtifact(artifacts, name) {
    const matches = artifacts.filter(item => item.name === name);
    if (matches.length !== 1) {
      throw new AdmissionError(`expected exactly one artifact named ${name}, found ${matches.length}`);
    }
    const artifact = matches[0];
    if (artifact.expired) {
      throw new AdmissionError(`artifact ${name} (id ${artifact.id}) is expired`);
    }
    if (!isPositiveInt(artifact.id)) {
      throw new AdmissionError(`artifact ${name} has an invalid id`);
    }
    return artifact;
  }

  async function resolvePrFromRun(apiUrl, token, owner, repo, run, expectedPrNumber) {
    // GitHub leaves workflow_run.pull_requests empty for fork PRs. When
    // present, use it as an extra guard; otherwise resolve the unique open PR
    // from the trusted workflow_run head repo/branch/SHA metadata.
    const workflowPrNumbers = new Set(
      (run.pull_requests || []).map(pr => Number(pr.number)).filter(isPositiveInt)
    );
    if (workflowPrNumbers.size > 0) {
      if (!workflowPrNumbers.has(expectedPrNumber)) {
        throw new AdmissionError(`PR #${expectedPrNumber} is not listed in run ${run.id}`);
      }
      return;
    }
    const headRepo = run.head_repository || {};
    const runHeadOwner = headRepo.owner && headRepo.owner.login;
    if (!runHeadOwner || !run.head_branch) {
      throw new AdmissionError('run head owner or branch is missing; cannot resolve the PR');
    }
    const data = await getJson(
      apiUrl, token,
      `/repos/${owner}/${repo}/pulls?state=open&head=${encodeURIComponent(`${runHeadOwner}:${run.head_branch}`)}&per_page=100`
    );
    if (!Array.isArray(data)) {
      throw new AdmissionError('PR listing returned a non-array payload');
    }
    const matching = data.filter(pr => (
      pr.head &&
      pr.head.repo &&
      pr.head.repo.full_name === (headRepo.full_name || `${runHeadOwner}/${headRepo.name || ''}`) &&
      pr.head.sha === run.head_sha &&
      pr.base &&
      pr.base.repo &&
      pr.base.repo.full_name === `${owner}/${repo}`
    ));
    if (matching.length !== 1) {
      throw new AdmissionError(`run matched ${matching.length} open PRs for ${runHeadOwner}:${run.head_branch}; expected exactly one`);
    }
    if (Number(matching[0].number) !== expectedPrNumber) {
      throw new AdmissionError(`artifact PR #${expectedPrNumber} does not match run PR #${matching[0].number}`);
    }
  }

  async function validatePrAdmission(apiUrl, token, owner, repo, run, metadata) {
    const prNumber = Number(metadata.pr_number);
    if (!isPositiveInt(prNumber)) {
      throw new AdmissionError(`invalid PR number in metadata: ${metadata.pr_number}`);
    }
    await resolvePrFromRun(apiUrl, token, owner, repo, run, prNumber);

    let pull;
    try {
      pull = await getJson(apiUrl, token, `/repos/${owner}/${repo}/pulls/${prNumber}`);
    } catch (error) {
      if (error instanceof AdmissionError && /HTTP 404/.test(error.message)) {
        throw new AdmissionError(`PR #${prNumber} no longer exists`);
      }
      throw error;
    }
    if (pull.state !== 'open') {
      throw new AdmissionError(`PR #${prNumber} is ${pull.state}; skipping stale run`);
    }
    const runHeadRepo = run.head_repository && run.head_repository.full_name;
    if (pull.base.repo.full_name !== `${owner}/${repo}` || pull.head.repo.full_name !== runHeadRepo) {
      throw new AdmissionError('current PR repository metadata does not match the run');
    }
    if (pull.head.sha !== run.head_sha) {
      throw new AdmissionError('current PR head SHA differs from the run; stale run');
    }

    // Label admission: only maintainers can apply/remove labels, so the label
    // currently present on the unchanged PR head is the admission evidence.
    // The metadata label (untrusted) must be one of the allowed labels and
    // must agree with the current label; any disagreement fails closed.
    const currentLabels = new Set((pull.labels || []).map(label => label && label.name));
    const allowedPresent = [...currentLabels].filter(name => ALLOWED_LABELS.has(name));
    if (allowedPresent.length !== 1) {
      throw new AdmissionError(`PR #${prNumber} must carry exactly one regression label (${[...ALLOWED_LABELS].join(', ')}); found: ${[...currentLabels].join(', ') || 'none'}`);
    }
    if (!ALLOWED_LABELS.has(metadata.label)) {
      throw new AdmissionError(`metadata label ${JSON.stringify(metadata.label)} is not an allowed regression label`);
    }
    if (metadata.label !== allowedPresent[0]) {
      throw new AdmissionError(`metadata label ${metadata.label} does not match the current PR label ${allowedPresent[0]}`);
    }
    return { label: allowedPresent[0], prNumber };
  }

  async function validateMergeParents(apiUrl, token, owner, repo, metadata) {
    // The candidate SHA is the PR merge commit. The REST repos.getCommit
    // response carries the commit SHA and its parents at the TOP level of the
    // response object (`sha`, `parents`); the nested `commit` object is the
    // git-commit payload and must not be relied on for these fields.
    const commit = await getJson(apiUrl, token, `/repos/${owner}/${repo}/commits/${metadata.candidate_sha}`);
    if (String(commit.sha).toLowerCase() !== String(metadata.candidate_sha).toLowerCase()) {
      throw new AdmissionError(
        `commit API returned ${commit.sha} for the requested candidate merge SHA ${metadata.candidate_sha}`
      );
    }
    const parents = commit.parents || [];
    const parentShas = parents.map(parent => String(parent.sha).toLowerCase());
    if (parentShas.length !== 2) {
      throw new AdmissionError(`candidate merge SHA ${metadata.candidate_sha} has ${parentShas.length} parents; expected 2`);
    }
    const headParents = parentShas.filter(sha => sha === String(metadata.head_sha).toLowerCase());
    if (headParents.length !== 1) {
      throw new AdmissionError(`candidate merge SHA must have exactly one parent equal to head SHA ${metadata.head_sha}`);
    }
    const baseParents = parentShas.filter(sha => sha === String(metadata.built_base_sha).toLowerCase());
    if (baseParents.length !== 1) {
      throw new AdmissionError(`candidate merge SHA must have exactly one parent equal to built base SHA ${metadata.built_base_sha}`);
    }
  }

  function validateCaller(env, run) {
    // workflow_call mode: the caller workflow and its triggering event/ref are
    // read from the github context of the controller run (runner-provided,
    // not caller inputs). Only the Release workflow may invoke the controller,
    // and only on the events/refs the release pipeline actually uses.
    if (!ALLOWED_CALLER_WORKFLOWS.has(env.CALLER_WORKFLOW)) {
      throw new AdmissionError(
        `caller workflow ${JSON.stringify(env.CALLER_WORKFLOW)} is not an allowed controller caller (${[...ALLOWED_CALLER_WORKFLOWS].join(', ')})`
      );
    }
    if (!ALLOWED_CALLER_EVENTS.has(env.CALLER_EVENT)) {
      throw new AdmissionError(
        `caller event ${JSON.stringify(env.CALLER_EVENT)} is not a release event (${[...ALLOWED_CALLER_EVENTS].join(', ')})`
      );
    }
    if (!isFullSha(env.CALLER_SHA)) {
      throw new AdmissionError(`caller SHA is not a full 40-hex digest: ${env.CALLER_SHA}`);
    }
    // The runner-provided caller context and the API-reported source run
    // describe the same enclosing Release run: their events must agree.
    if (run.event !== env.CALLER_EVENT) {
      throw new AdmissionError(
        `source run event ${JSON.stringify(run.event)} does not match the caller event ${JSON.stringify(env.CALLER_EVENT)}`
      );
    }
    // The build run the controller follows must have built exactly the
    // caller's commit (reusable runs inherit the caller's head SHA).
    if (String(run.head_sha).toLowerCase() !== String(env.CALLER_SHA).toLowerCase()) {
      throw new AdmissionError(
        `source run head SHA ${run.head_sha} does not match the caller SHA ${env.CALLER_SHA}`
      );
    }
    const defaultBranch = env.CALLER_DEFAULT_BRANCH || 'main';
    const ref = env.CALLER_REF || '';
    const refType = env.CALLER_REF_TYPE || '';
    if (env.CALLER_EVENT === 'push') {
      // Tag push releases: the release tag is the only admissible push ref.
      if (refType !== 'tag' || !RELEASE_TAG_REF_RE.test(ref)) {
        throw new AdmissionError(`push caller ref ${JSON.stringify(ref)} (${refType}) is not a v*.*.* release tag`);
      }
    } else if (env.CALLER_EVENT === 'schedule') {
      // Scheduled releases build the default branch.
      if (refType !== 'branch' || ref !== `refs/heads/${defaultBranch}`) {
        throw new AdmissionError(`schedule caller ref ${JSON.stringify(ref)} (${refType}) is not the default branch`);
      }
    } else {
      // workflow_dispatch: exactly two admissible forms — a release tag
      // (refType=tag and refs/tags/vN.N.N) or the default branch
      // (refType=branch and refs/heads/<default_branch>). Arbitrary tags,
      // other branches, mismatched type/ref pairs, and malformed refs all
      // fail closed.
      if (refType === 'tag') {
        if (!RELEASE_TAG_REF_RE.test(ref)) {
          throw new AdmissionError(`workflow_dispatch caller ref ${JSON.stringify(ref)} (${refType}) is not a release tag refs/tags/v*.*.*`);
        }
      } else if (refType === 'branch') {
        if (ref !== `refs/heads/${defaultBranch}`) {
          throw new AdmissionError(`workflow_dispatch caller ref ${JSON.stringify(ref)} (${refType}) is not the default branch`);
        }
      } else {
        throw new AdmissionError(`workflow_dispatch caller ref ${JSON.stringify(ref)} (${refType}) is neither a tag nor a branch ref`);
      }
    }
  }

  function validateCallInputs(env, validated) {
    // workflow_call mode: the caller passes the exact build outputs as typed
    // inputs (verified-base-sha/artifact ids from the build job outputs).
    // Each input must equal the value validated from the metadata artifact /
    // GitHub API so a caller can neither drift from the build nor replay a
    // different build.
    if (String(env.INPUT_BASE_SHA || '').toLowerCase() !== validated.built_base_sha) {
      throw new AdmissionError(
        `input base SHA ${JSON.stringify(env.INPUT_BASE_SHA)} does not match the validated build base SHA ${validated.built_base_sha}`
      );
    }
    if (String(env.INPUT_CANDIDATE_SHA || '').toLowerCase() !== validated.candidate_sha) {
      throw new AdmissionError(
        `input candidate SHA ${JSON.stringify(env.INPUT_CANDIDATE_SHA)} does not match the validated candidate SHA ${validated.candidate_sha}`
      );
    }
    if (Number(env.INPUT_BASE_ARTIFACT_ID) !== validated.base_artifact_id) {
      throw new AdmissionError(
        `input base artifact id ${JSON.stringify(env.INPUT_BASE_ARTIFACT_ID)} does not match the validated base artifact id ${validated.base_artifact_id}`
      );
    }
    if (Number(env.INPUT_CANDIDATE_ARTIFACT_ID) !== validated.candidate_artifact_id) {
      throw new AdmissionError(
        `input candidate artifact id ${JSON.stringify(env.INPUT_CANDIDATE_ARTIFACT_ID)} does not match the validated candidate artifact id ${validated.candidate_artifact_id}`
      );
    }
    if (env.INPUT_CASE && env.INPUT_CASE !== validated.case) {
      throw new AdmissionError(
        `input case ${JSON.stringify(env.INPUT_CASE)} does not match the validated case ${validated.case}`
      );
    }
  }

  async function validate(env) {
    const apiUrl = env.GITHUB_API_URL;
    const token = env.GITHUB_TOKEN;
    const repository = env.GITHUB_REPOSITORY;
    const expectedRunId = Number(env.SOURCE_RUN_ID);
    const expectedRunAttempt = Number(env.SOURCE_RUN_ATTEMPT);
    const expectedWorkflow = env.SOURCE_WORKFLOW_NAME || DEFAULT_SOURCE_WORKFLOW;
    const sourceMode = env.SOURCE_MODE === 'workflow_call' ? 'workflow_call' : 'workflow_run';

    if (!apiUrl || !token || !repository) {
      throw new AdmissionError('GITHUB_API_URL, GITHUB_TOKEN and GITHUB_REPOSITORY are required');
    }
    if (sourceMode === 'workflow_call' && !env.CALLER_WORKFLOW) {
      throw new AdmissionError('SOURCE_MODE=workflow_call requires CALLER_WORKFLOW from the github context');
    }
    if (!isPositiveInt(expectedRunId)) {
      throw new AdmissionError(`SOURCE_RUN_ID must be a positive integer: ${env.SOURCE_RUN_ID}`);
    }
    if (!isPositiveInt(expectedRunAttempt)) {
      throw new AdmissionError(`SOURCE_RUN_ATTEMPT must be a positive integer: ${env.SOURCE_RUN_ATTEMPT}`);
    }
    if (!/^[^/]+\/[^/]+$/.test(repository)) {
      throw new AdmissionError(`GITHUB_REPOSITORY must be owner/repo: ${repository}`);
    }
    const [owner, repo] = repository.split('/');

    // 1. The originating run itself. The two source modes describe different
    //    run shapes:
    //    - workflow_run: a completed, SUCCESSFUL `Query Regression` build run
    //      followed by the controller workflow;
    //    - workflow_call: the enclosing `Release` run that is STILL EXECUTING
    //      the controller as one of its reusable-workflow jobs. Its name is
    //      `Release` (not `Query Regression`) and its conclusion is null/empty
    //      (not `success`) at admission time.
    const run = await getJson(apiUrl, token, `/repos/${owner}/${repo}/actions/runs/${expectedRunId}`);
    if (run.id !== expectedRunId) {
      throw new AdmissionError(`run id mismatch: API returned ${run.id}, expected ${expectedRunId}`);
    }
    if (!run.repository || run.repository.full_name !== repository) {
      throw new AdmissionError(`run ${run.id} belongs to ${run.repository && run.repository.full_name}, not ${repository}`);
    }
    if (Number(run.run_attempt) !== expectedRunAttempt) {
      throw new AdmissionError(`run ${run.id} attempt ${run.run_attempt} does not match expected attempt ${expectedRunAttempt}`);
    }
    if (!isFullSha(run.head_sha)) {
      throw new AdmissionError(`run ${run.id} head_sha is not a full SHA: ${run.head_sha}`);
    }
    if (sourceMode === 'workflow_call') {
      // Actual reusable-workflow semantics: the build and the controller are
      // jobs in the same enclosing Release run, which must still be running
      // (in_progress, no conclusion yet) while the controller executes.
      if (run.name !== 'Release' || canonicalRunPath(run.path) !== RELEASE_WORKFLOW_PATH) {
        throw new AdmissionError(
          `run ${run.id} is ${JSON.stringify(run.name)} at ${JSON.stringify(run.path)}, not the enclosing Release workflow at ${RELEASE_WORKFLOW_PATH}`
        );
      }
      if (!ALLOWED_CALLER_EVENTS.has(run.event)) {
        throw new AdmissionError(
          `run ${run.id} event ${JSON.stringify(run.event)} is not a Release event (${[...ALLOWED_CALLER_EVENTS].join(', ')})`
        );
      }
      if (run.status !== 'in_progress') {
        throw new AdmissionError(
          `run ${run.id} status is ${JSON.stringify(run.status)}; the enclosing Release run must still be in_progress while the controller executes`
        );
      }
      if (run.conclusion !== null && run.conclusion !== '') {
        throw new AdmissionError(
          `run ${run.id} conclusion is ${JSON.stringify(run.conclusion)}; the enclosing Release run must not be concluded while the controller executes`
        );
      }
      // The source run id/attempt must equal the controller's own run
      // id/attempt: both the build and the controller are jobs in the same
      // enclosing Release run.
      const controllerRunId = Number(env.CONTROLLER_RUN_ID);
      const controllerRunAttempt = Number(env.CONTROLLER_RUN_ATTEMPT);
      if (!isPositiveInt(controllerRunId) || controllerRunId !== expectedRunId) {
        throw new AdmissionError(
          `CONTROLLER_RUN_ID ${JSON.stringify(env.CONTROLLER_RUN_ID)} does not equal the source run id ${expectedRunId}; the build and the controller must run in the same Release run`
        );
      }
      if (!isPositiveInt(controllerRunAttempt) || controllerRunAttempt !== expectedRunAttempt) {
        throw new AdmissionError(
          `CONTROLLER_RUN_ATTEMPT ${JSON.stringify(env.CONTROLLER_RUN_ATTEMPT)} does not equal the source run attempt ${expectedRunAttempt}; the build and the controller must run in the same Release run attempt`
        );
      }
    } else {
      // workflow_run mode: retain the exact completed/successful Query
      // Regression validation (pull_request label runs and standalone
      // workflow_dispatch builds).
      if (run.name !== expectedWorkflow) {
        throw new AdmissionError(`run ${run.id} belongs to workflow ${JSON.stringify(run.name)}, expected ${JSON.stringify(expectedWorkflow)}`);
      }
      if (canonicalRunPath(run.path) !== SOURCE_WORKFLOW_PATH) {
        throw new AdmissionError(
          `run ${run.id} is at ${JSON.stringify(run.path)}, not the Query Regression workflow at ${SOURCE_WORKFLOW_PATH}`
        );
      }
      if (run.conclusion !== 'success') {
        throw new AdmissionError(`run ${run.id} conclusion is ${run.conclusion}; only success is admissible`);
      }
      if (!ALLOWED_EVENTS.has(run.event)) {
        throw new AdmissionError(`run ${run.id} event ${JSON.stringify(run.event)} is not admissible (${[...ALLOWED_EVENTS].join(', ')})`);
      }
    }

    // 2. Artifacts of the originating run.
    const artifacts = await listArtifacts(apiUrl, token, owner, repo, expectedRunId);
    const metadataArtifact = findArtifact(artifacts, ARTIFACT_NAMES.metadata);
    const baseArtifact = findArtifact(artifacts, ARTIFACT_NAMES.base);
    const candidateArtifact = findArtifact(artifacts, ARTIFACT_NAMES.candidate);

    // 3. The metadata artifact (untrusted) — every cross-checkable field must
    //    agree with the API. The controller workflow downloads it by the id
    //    validated here. This script downloads the metadata artifact zip itself
    //    and reads it directly, so the zip handling stays out of the workflow.
    let metadata;
    {
      const raw = await getBuffer(
        apiUrl, token,
        `/repos/${owner}/${repo}/actions/artifacts/${metadataArtifact.id}/zip`
      );
      const decoded = unzipFirstEntry(raw, 'query-regression-metadata.json');
      try {
        metadata = JSON.parse(decoded.toString('utf8'));
      } catch (error) {
        throw new AdmissionError(`metadata artifact is not valid JSON: ${error.message}`);
      }
    }
    if (metadata === null || typeof metadata !== 'object' || Array.isArray(metadata)) {
      throw new AdmissionError('metadata artifact is not a JSON object');
    }
    if (metadata.schema_version !== 1) {
      throw new AdmissionError(`metadata schema_version ${metadata.schema_version} is not supported`);
    }
    if (Number(metadata.run_id) !== expectedRunId || Number(metadata.run_attempt) !== expectedRunAttempt) {
      throw new AdmissionError('metadata run id/attempt does not match the originating run (possible replay)');
    }
    if (metadata.repository !== repository) {
      throw new AdmissionError(`metadata repository ${metadata.repository} does not match ${repository}`);
    }
    if (!ALLOWED_METADATA_EVENTS.has(metadata.event)) {
      throw new AdmissionError(`metadata event ${JSON.stringify(metadata.event)} is not admissible`);
    }
    // The metadata is written by the build run itself, so its recorded event
    // must equal the API-reported run event exactly (covers pull_request,
    // workflow_dispatch, and every release/reusable-call event).
    if (metadata.event !== run.event) {
      throw new AdmissionError(`metadata event ${metadata.event} does not match run event ${run.event}`);
    }
    if (String(metadata.head_sha).toLowerCase() !== String(run.head_sha).toLowerCase()) {
      throw new AdmissionError(`metadata head SHA ${metadata.head_sha} does not match run head SHA ${run.head_sha}`);
    }
    if (!isFullSha(metadata.built_base_sha) || !isFullSha(metadata.candidate_sha)) {
      throw new AdmissionError('metadata built_base_sha/candidate_sha must be full SHAs');
    }
    if (Number(metadata.base_artifact_id) !== baseArtifact.id) {
      throw new AdmissionError(`metadata base artifact id ${metadata.base_artifact_id} does not match run artifact id ${baseArtifact.id}`);
    }
    if (Number(metadata.candidate_artifact_id) !== candidateArtifact.id) {
      throw new AdmissionError(`metadata candidate artifact id ${metadata.candidate_artifact_id} does not match run artifact id ${candidateArtifact.id}`);
    }

    const validated = {
      schema_version: 1,
      source_run_id: expectedRunId,
      source_run_attempt: expectedRunAttempt,
      repository,
      event: run.event,
      head_sha: String(run.head_sha).toLowerCase(),
      built_base_sha: String(metadata.built_base_sha).toLowerCase(),
      candidate_sha: String(metadata.candidate_sha).toLowerCase(),
      event_base_sha: metadata.event_base_sha ? String(metadata.event_base_sha).toLowerCase() : null,
      base_repo: repository,
      head_repo: run.head_repository && run.head_repository.full_name,
      label: null,
      pr_number: null,
      case: null,
      base_artifact_id: baseArtifact.id,
      candidate_artifact_id: candidateArtifact.id,
      metadata_artifact_id: metadataArtifact.id,
      run_url: run.html_url || `${apiUrl.replace(/\/api\.github\.com/, 'https://github.com')}/${repository}/actions/runs/${expectedRunId}`,
      validated_at: new Date().toISOString(),
    };
    if (run.event === 'pull_request') {
      const pr = await validatePrAdmission(apiUrl, token, owner, repo, run, metadata);
      await validateMergeParents(apiUrl, token, owner, repo, metadata);
      validated.pr_number = pr.prNumber;
      validated.label = pr.label;
      if (metadata.head_repo && validated.head_repo && metadata.head_repo !== validated.head_repo) {
        throw new AdmissionError(`metadata head repo ${metadata.head_repo} does not match run head repo ${validated.head_repo}`);
      }
      // Case paths derive from the validated label; the untrusted metadata
      // must agree (fail closed on any disagreement).
      const expectedCase = pr.label === 'heavy-regression' ? 'heavy' : 'all';
      if (metadata.case !== expectedCase) {
        throw new AdmissionError(`metadata case ${JSON.stringify(metadata.case)} does not match label ${pr.label} (expected ${expectedCase})`);
      }
      validated.case = expectedCase;
    } else {
      // workflow_dispatch / release (push/schedule/workflow_call): the
      // triggering event is the maintainer admission; there is no PR to
      // re-validate and no merge SHA.
      log(`non-PR event ${run.event}: dispatch/release admission by the triggering event`);
      if (typeof metadata.case !== 'string' || metadata.case === '') {
        throw new AdmissionError('metadata case must be a non-empty string for non-PR runs');
      }
      validated.case = metadata.case;
    }

    // 4. Caller mode: the controller was invoked explicitly by release.yml
    //    (workflow_call). Validate the caller context and the typed inputs
    //    against the values just validated from the build run.
    if (sourceMode === 'workflow_call') {
      validateCaller(env, run);
      validateCallInputs(env, validated);
    }

    return { ok: true, validated };
  }

  return { validate };
}

function appendOutput(env, key, value) {
  const outputPath = env.GITHUB_OUTPUT;
  if (!outputPath) {
    return;
  }
  fs.appendFileSync(outputPath, `${key}=${value}\n`, 'utf8');
}

function runAdmission(env = process.env) {
  const validator = createValidator({ fetchImpl: globalThis.fetch });
  return validator.validate(env).then(({ validated }) => {
    if (env.VALIDATION_OUTPUT) {
      fs.writeFileSync(env.VALIDATION_OUTPUT, JSON.stringify(validated, null, 2) + '\n', 'utf8');
    }
    appendOutput(env, 'base-artifact-id', validated.base_artifact_id);
    appendOutput(env, 'candidate-artifact-id', validated.candidate_artifact_id);
    appendOutput(env, 'metadata-artifact-id', validated.metadata_artifact_id);
    appendOutput(env, 'source-run-id', validated.source_run_id);
    appendOutput(env, 'source-event', validated.event);
    appendOutput(env, 'case-paths', validated.case || 'all');
    process.stdout.write(`admission ok: run ${validated.source_run_id} attempt ${validated.source_run_attempt} event ${validated.event}${validated.pr_number ? ` pr #${validated.pr_number}` : ''}\n`);
  });
}

if (require.main === module) {
  runAdmission().catch(error => {
    process.stderr.write(`admission rejected: ${error.message}\n`);
    process.exitCode = 1;
  });
}

module.exports = {
  createValidator,
  unzipFirstEntry,
  runAdmission,
  isFullSha,
  canonicalRunPath,
  ALLOWED_EVENTS,
  ALLOWED_LABELS,
  ALLOWED_CALLER_WORKFLOWS,
  ALLOWED_CALLER_EVENTS,
  RELEASE_WORKFLOW_PATH,
  SOURCE_WORKFLOW_PATH,
  RELEASE_TAG_REF_RE,
  ARTIFACT_NAMES,
  AdmissionError,
  DEFAULT_SOURCE_WORKFLOW,
};

module.exports._test = {
  unzipFirstEntry,
};
