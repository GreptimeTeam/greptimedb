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

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const zlib = require('zlib');

const admission = require('./query-regression-admission.cjs');
const { createValidator, unzipFirstEntry, ALLOWED_LABELS } = admission;

const OWNER = 'GreptimeTeam';
const REPO = 'greptimedb';
const REPOSITORY = `${OWNER}/${REPO}`;
const HEAD_SHA = 'a'.repeat(40);
const BASE_SHA = 'b'.repeat(40);
const MERGE_SHA = 'c'.repeat(40);
const RUN_ID = 9001;
const RUN_ATTEMPT = 1;
const BASE_ARTIFACT_ID = 101;
const CANDIDATE_ARTIFACT_ID = 102;
const METADATA_ARTIFACT_ID = 103;

// ---------------------------------------------------------------------------
// Fake GitHub API
// ---------------------------------------------------------------------------

function zipOf(entries) {
  // Builds a minimal valid zip (stored entries) for the metadata artifact.
  const parts = [];
  const central = [];
  let offset = 0;
  for (const [name, content] of Object.entries(entries)) {
    const nameBuf = Buffer.from(name, 'utf8');
    const dataBuf = Buffer.from(content, 'utf8');
    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4); // version needed
    local.writeUInt16LE(0, 6); // flags
    local.writeUInt16LE(0, 8); // method: stored
    local.writeUInt16LE(0x21, 10); // mod time
    local.writeUInt16LE(0x0221, 12); // mod date
    local.writeUInt32LE(0, 14); // crc-32 (unused by the reader)
    local.writeUInt32LE(dataBuf.length, 18); // compressed size
    local.writeUInt32LE(dataBuf.length, 22); // uncompressed size
    local.writeUInt16LE(nameBuf.length, 26);
    local.writeUInt16LE(0, 28); // extra len
    parts.push(local, nameBuf, dataBuf);
    const cd = Buffer.alloc(46);
    cd.writeUInt32LE(0x02014b50, 0);
    cd.writeUInt16LE(20, 4); // version made by
    cd.writeUInt16LE(20, 6); // version needed
    cd.writeUInt16LE(0, 8); // flags
    cd.writeUInt16LE(0, 10); // method
    cd.writeUInt16LE(0x21, 12);
    cd.writeUInt16LE(0x0221, 14);
    cd.writeUInt32LE(0, 16);
    cd.writeUInt32LE(dataBuf.length, 20);
    cd.writeUInt32LE(dataBuf.length, 24);
    cd.writeUInt16LE(nameBuf.length, 28);
    cd.writeUInt16LE(0, 30);
    cd.writeUInt16LE(0, 32);
    cd.writeUInt16LE(0, 34);
    cd.writeUInt16LE(0, 36);
    cd.writeUInt32LE(0, 38);
    cd.writeUInt32LE(offset, 42);
    central.push(cd, nameBuf);
    offset += 30 + nameBuf.length + dataBuf.length;
  }
  const cdSize = central.reduce((sum, buf) => sum + buf.length, 0);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(central.length / 2, 8);
  eocd.writeUInt16LE(central.length / 2, 10);
  eocd.writeUInt32LE(cdSize, 12);
  eocd.writeUInt32LE(offset, 16);
  eocd.writeUInt16LE(0, 20);
  return Buffer.concat([...parts, ...central, eocd]);
}

function metadataArtifactZip(overrides = {}) {
  const metadata = {
    schema_version: 1,
    repository: REPOSITORY,
    run_id: RUN_ID,
    run_attempt: RUN_ATTEMPT,
    event: 'pull_request',
    pr_number: 42,
    head_sha: HEAD_SHA,
    event_base_sha: BASE_SHA,
    built_base_sha: BASE_SHA,
    candidate_sha: MERGE_SHA,
    head_repo: REPOSITORY,
    base_repo: REPOSITORY,
    label: 'query-regression',
    case: 'all',
    base_artifact_id: BASE_ARTIFACT_ID,
    candidate_artifact_id: CANDIDATE_ARTIFACT_ID,
    ...overrides,
  };
  return zipOf({ 'query-regression-metadata.json': JSON.stringify(metadata) });
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

// Builds a fake fetch that routes the same URL patterns the admission script
// calls. `state` lets tests mutate run/PR/artifact behavior.
function makeApi({ runOverrides = {}, prOverrides = {}, artifactOverrides = {} } = {}) {
  const state = {
    run: {
      id: RUN_ID,
      name: 'Query Regression',
      path: '.github/workflows/query-regression.yml',
      status: 'completed',
      repository: { full_name: REPOSITORY },
      run_attempt: RUN_ATTEMPT,
      conclusion: 'success',
      event: 'pull_request',
      head_sha: HEAD_SHA,
      head_branch: 'feature/x',
      head_repository: { full_name: REPOSITORY, owner: { login: OWNER }, name: REPO },
      pull_requests: [{ number: 42 }],
      html_url: `https://github.com/${REPOSITORY}/actions/runs/${RUN_ID}`,
      ...runOverrides,
    },
    pr: {
      number: 42,
      state: 'open',
      head: { sha: HEAD_SHA, repo: { full_name: REPOSITORY } },
      base: { repo: { full_name: REPOSITORY } },
      labels: [{ name: 'query-regression' }],
      ...prOverrides,
    },
    mergeParents: [HEAD_SHA, BASE_SHA],
    commitShaOverride: null,
    artifacts: [
      { id: METADATA_ARTIFACT_ID, name: 'query-regression-metadata', expired: false },
      { id: BASE_ARTIFACT_ID, name: 'query-regression-base-binaries', expired: false },
      { id: CANDIDATE_ARTIFACT_ID, name: 'query-regression-candidate-binaries', expired: false },
      ...(Array.isArray(artifactOverrides) ? artifactOverrides : []),
    ],
    metadataZip: metadataArtifactZip(),
    pullsForHead: [],
  };

  async function fetchImpl(url) {
    const u = new URL(url);
    const pathname = u.pathname;
    if (pathname === `/repos/${REPOSITORY}/actions/runs/${RUN_ID}`) {
      return jsonResponse(state.run);
    }
    const artifactList = pathname.match(/^\/repos\/[^/]+\/[^/]+\/actions\/runs\/\d+\/artifacts$/);
    if (artifactList) {
      return jsonResponse({ total_count: state.artifacts.length, artifacts: state.artifacts });
    }
    const artifactZip = pathname.match(/^\/repos\/[^/]+\/[^/]+\/actions\/artifacts\/(\d+)\/zip$/);
    if (artifactZip) {
      return new Response(state.metadataZip, { status: 200 });
    }
    const pullsList = pathname.match(/^\/repos\/[^/]+\/[^/]+\/pulls$/);
    if (pullsList) {
      return jsonResponse(state.pullsForHead);
    }
    const pullsGet = pathname.match(/^\/repos\/[^/]+\/[^/]+\/pulls\/(\d+)$/);
    if (pullsGet) {
      return jsonResponse(state.pr);
    }
    const commit = pathname.match(/^\/repos\/[^/]+\/[^/]+\/commits\/([0-9a-f]+)$/);
    if (commit) {
      // Real REST repos.getCommit shape: the commit SHA and parents live at
      // the TOP level of the response (`sha`, `parents`); the nested `commit`
      // object is the git-commit payload.
      const sha = state.commitShaOverride || commit[1];
      const parents = state.mergeParents.map(sha => ({ sha, url: `https://api.github.com/repos/${REPOSITORY}/commits/${sha}` }));
      return jsonResponse({
        sha,
        commit: { sha, message: 'merge commit', tree: { sha: 'd'.repeat(40) }, comment_count: 0 },
        parents,
      });
    }
    return jsonResponse({ message: `unhandled ${pathname}` }, 404);
  }

  return { fetchImpl, state };
}

function envFor(overrides = {}) {
  return {
    GITHUB_API_URL: 'https://api.github.com',
    GITHUB_TOKEN: 'token',
    GITHUB_REPOSITORY: REPOSITORY,
    SOURCE_RUN_ID: String(RUN_ID),
    SOURCE_RUN_ATTEMPT: String(RUN_ATTEMPT),
    ...overrides,
  };
}

async function expectRejected(api, envOverrides = {}, messagePart) {
  const { fetchImpl } = api;
  const validator = createValidator({ fetchImpl });
  await assert.rejects(
    () => validator.validate(envFor(envOverrides)),
    error => {
      assert.ok(error instanceof admission.AdmissionError, `expected AdmissionError, got ${error}`);
      if (messagePart) {
        assert.match(error.message, messagePart);
      }
      return true;
    }
  );
}

async function expectAccepted(api, envOverrides = {}) {
  const { fetchImpl } = api;
  const validator = createValidator({ fetchImpl });
  const result = await validator.validate(envFor(envOverrides));
  assert.equal(result.ok, true);
  return result.validated;
}

// ---------------------------------------------------------------------------
// Zip reader
// ---------------------------------------------------------------------------

test('unzips a stored metadata artifact and finds the wanted file', () => {
  const zip = zipOf({ 'query-regression-metadata.json': '{"a":1}' });
  const decoded = unzipFirstEntry(zip, 'query-regression-metadata.json');
  assert.equal(decoded.toString('utf8'), '{"a":1}');
});

test('unzips a deflated entry', () => {
  const raw = Buffer.from('{"nested":true}');
  const deflated = zlib.deflateRawSync(raw);
  const nameBuf = Buffer.from('query-regression-metadata.json');
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4);
  local.writeUInt16LE(0, 6);
  local.writeUInt16LE(8, 8); // deflate
  local.writeUInt32LE(deflated.length, 18);
  local.writeUInt32LE(raw.length, 22);
  local.writeUInt16LE(nameBuf.length, 26);
  const cd = Buffer.alloc(46);
  cd.writeUInt32LE(0x02014b50, 0);
  cd.writeUInt16LE(20, 4);
  cd.writeUInt16LE(20, 6);
  cd.writeUInt16LE(8, 10);
  cd.writeUInt32LE(deflated.length, 20);
  cd.writeUInt32LE(raw.length, 24);
  cd.writeUInt16LE(nameBuf.length, 28);
  cd.writeUInt32LE(0, 42);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(1, 8);
  eocd.writeUInt16LE(1, 10);
  eocd.writeUInt32LE(cd.length, 12);
  eocd.writeUInt32LE(30 + nameBuf.length + deflated.length, 16);
  const zip = Buffer.concat([local, nameBuf, deflated, cd, nameBuf, eocd]);
  const decoded = unzipFirstEntry(zip, 'query-regression-metadata.json');
  assert.equal(decoded.toString('utf8'), '{"nested":true}');
});

test('rejects a truncated or non-zip buffer', () => {
  assert.throws(() => unzipFirstEntry(Buffer.from('not a zip at all, sorry'), 'x.json'), /not a valid zip/);
});

// ---------------------------------------------------------------------------
// Acceptance paths
// ---------------------------------------------------------------------------

test('accepts a same-repo labeled PR run', async () => {
  const api = makeApi();
  const validated = await expectAccepted(api);
  assert.equal(validated.source_run_id, RUN_ID);
  assert.equal(validated.pr_number, 42);
  assert.equal(validated.label, 'query-regression');
  assert.equal(validated.base_artifact_id, BASE_ARTIFACT_ID);
  assert.equal(validated.candidate_artifact_id, CANDIDATE_ARTIFACT_ID);
  assert.equal(validated.head_sha, HEAD_SHA);
  assert.equal(validated.built_base_sha, BASE_SHA);
  assert.equal(validated.candidate_sha, MERGE_SHA);
});

test('accepts a fork PR via the empty pull_requests fallback', async () => {
  const fork = 'octocat';
  const api = makeApi({
    runOverrides: {
      head_repository: { full_name: `${fork}/greptimedb`, owner: { login: fork }, name: REPO },
      pull_requests: [], // GitHub leaves this empty for fork PRs
    },
    prOverrides: {
      head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } },
    },
  });
  api.state.pullsForHead = [{
    number: 42,
    head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } },
    base: { repo: { full_name: REPOSITORY } },
  }];
  api.state.metadataZip = metadataArtifactZip({ head_repo: `${fork}/greptimedb` });
  const validated = await expectAccepted(api);
  assert.equal(validated.pr_number, 42);
  assert.equal(validated.head_repo, `${fork}/greptimedb`);
});

test('accepts a workflow_dispatch run without PR checks', async () => {
  const api = makeApi({
    runOverrides: {
      event: 'workflow_dispatch',
      head_branch: 'main',
      pull_requests: [],
    },
  });
  api.state.metadataZip = metadataArtifactZip({
    event: 'workflow_dispatch',
    pr_number: null,
    label: null,
    head_repo: REPOSITORY,
    base_repo: REPOSITORY,
    event_base_sha: null,
    candidate_sha: HEAD_SHA,
  });
  const validated = await expectAccepted(api);
  assert.equal(validated.event, 'workflow_dispatch');
  assert.equal(validated.pr_number, null);
  assert.equal(validated.label, null);
});

test('accepts a workflow_run build run whose API event is a tag push (workflow_run retains release-only events)', async () => {
  // workflow_run mode keeps accepting completed/successful 'Query Regression'
  // runs for release-only events (the controller workflow's `if:` gate skips
  // them in production; the admission script retains the superset). The
  // workflow_call release path is validated separately below.
  const api = makeApi({
    runOverrides: {
      event: 'push',
      head_branch: 'refs/tags/v1.2.3',
      pull_requests: [],
    },
  });
  // The build run records its own event (GITHUB_EVENT_NAME); for a reusable
  // release build the run and its metadata report the caller's event ('push').
  api.state.metadataZip = metadataArtifactZip({
    event: 'push',
    pr_number: null,
    label: null,
    head_repo: REPOSITORY,
    base_repo: REPOSITORY,
    event_base_sha: null,
    candidate_sha: HEAD_SHA,
  });
  const validated = await expectAccepted(api);
  assert.equal(validated.event, 'push');
  assert.equal(validated.candidate_sha, HEAD_SHA);
});

test('accepts a workflow_run build run whose REST path is unqualified or ref-qualified', async () => {
  // GitHub's REST API appends the triggering ref to `path`
  // (`.github/workflows/x.yml@refs/...`); admission canonicalizes by
  // stripping the first `@ref` suffix and requires the exact workflow file.
  for (const path of [
    '.github/workflows/query-regression.yml', // unqualified
    '.github/workflows/query-regression.yml@refs/heads/main', // branch-qualified
    '.github/workflows/query-regression.yml@refs/tags/v1.2.3', // tag-qualified
    `.github/workflows/query-regression.yml@${HEAD_SHA}`, // SHA-qualified
  ]) {
    const api = makeApi({ runOverrides: { path } });
    const validated = await expectAccepted(api);
    assert.equal(validated.source_run_id, RUN_ID);
    assert.equal(validated.event, 'pull_request');
  }
});

test('accepts a heavy-regression label', async () => {
  const api = makeApi({ prOverrides: { labels: [{ name: 'heavy-regression' }] } });
  api.state.metadataZip = metadataArtifactZip({ label: 'heavy-regression', case: 'heavy' });
  const validated = await expectAccepted(api);
  assert.equal(validated.label, 'heavy-regression');
  assert.equal(validated.case, 'heavy');
});

test('accepts merge parents in either order', async () => {
  const api = makeApi();
  api.state.mergeParents = [BASE_SHA, HEAD_SHA]; // base first
  const validated = await expectAccepted(api);
  assert.equal(validated.candidate_sha, MERGE_SHA);
});

// ---------------------------------------------------------------------------
// workflow_call (release) mode
// ---------------------------------------------------------------------------

// Actual same-Release-run shape: in workflow_call mode the source run is the
// ENCLOSING Release run — the build and the controller are reusable-workflow
// jobs in that same run. While the controller executes, the enclosing run is
// in_progress with a null conclusion, its name/path are exactly the Release
// workflow (NOT 'Query Regression'), and its event is the caller's triggering
// event (push/schedule/workflow_dispatch); candidate == the release commit.
function releaseApi(runOverrides = {}, metadataOverrides = {}) {
  const api = makeApi({
    runOverrides: {
      name: 'Release',
      path: '.github/workflows/release.yml',
      status: 'in_progress',
      conclusion: null,
      event: 'push',
      head_branch: 'refs/tags/v1.2.3',
      head_sha: HEAD_SHA,
      pull_requests: [],
      ...runOverrides,
    },
  });
  api.state.metadataZip = metadataArtifactZip({
    event: 'push',
    pr_number: null,
    label: null,
    head_repo: REPOSITORY,
    base_repo: REPOSITORY,
    event_base_sha: null,
    candidate_sha: HEAD_SHA,
    case: 'all',
    ...metadataOverrides,
  });
  return api;
}

// Full env for an explicit workflow_call invocation from release.yml. The
// CALLER_* values mirror the github context of the controller run (not
// caller inputs); the INPUT_* values are the typed workflow inputs;
// CONTROLLER_RUN_ID/ATTEMPT are the controller's own github run id/attempt,
// which must equal the source run's (same enclosing Release run).
function callEnv(overrides = {}) {
  return envFor({
    SOURCE_MODE: 'workflow_call',
    CONTROLLER_RUN_ID: String(RUN_ID),
    CONTROLLER_RUN_ATTEMPT: String(RUN_ATTEMPT),
    CALLER_WORKFLOW: 'Release',
    CALLER_EVENT: 'push',
    CALLER_REF: 'refs/tags/v1.2.3',
    CALLER_REF_TYPE: 'tag',
    CALLER_SHA: HEAD_SHA,
    CALLER_DEFAULT_BRANCH: 'main',
    INPUT_BASE_SHA: BASE_SHA,
    INPUT_CANDIDATE_SHA: HEAD_SHA,
    INPUT_BASE_ARTIFACT_ID: String(BASE_ARTIFACT_ID),
    INPUT_CANDIDATE_ARTIFACT_ID: String(CANDIDATE_ARTIFACT_ID),
    INPUT_CASE: 'all',
    ...overrides,
  });
}

test('accepts a workflow_call invocation with valid caller context and matching inputs', async () => {
  const api = releaseApi();
  const validated = await expectAccepted(api, callEnv());
  assert.equal(validated.event, 'push');
  assert.equal(validated.candidate_sha, HEAD_SHA);
  assert.equal(validated.case, 'all');
});

test('accepts a workflow_call invocation whose Release run path is unqualified or ref-qualified', async () => {
  // The enclosing Release run's REST `path` carries the triggering `@ref`
  // suffix; admission strips it and requires the exact release workflow file.
  for (const path of [
    '.github/workflows/release.yml', // unqualified
    '.github/workflows/release.yml@refs/tags/v1.2.3', // tag-qualified
    '.github/workflows/release.yml@refs/heads/main', // branch-qualified
    `.github/workflows/release.yml@${HEAD_SHA}`, // SHA-qualified
  ]) {
    const api = releaseApi({ path });
    const validated = await expectAccepted(api, callEnv());
    assert.equal(validated.event, 'push');
  }
});

test('accepts a workflow_call invocation for schedule and workflow_dispatch callers', async () => {
  // workflow_dispatch admits exactly two ref forms: a release tag
  // (refType=tag, refs/tags/vN.N.N) or the default branch (refType=branch,
  // refs/heads/<default_branch>).
  for (const [event, ref, refType] of [
    ['schedule', 'refs/heads/main', 'branch'],
    ['workflow_dispatch', 'refs/tags/v1.2.3', 'tag'],
    ['workflow_dispatch', 'refs/heads/main', 'branch'],
  ]) {
    const api = releaseApi({ event, head_branch: ref });
    api.state.metadataZip = metadataArtifactZip({
      event, pr_number: null, label: null, head_repo: REPOSITORY, base_repo: REPOSITORY,
      event_base_sha: null, candidate_sha: HEAD_SHA, case: 'all',
    });
    const validated = await expectAccepted(api, callEnv({
      CALLER_EVENT: event, CALLER_REF: ref, CALLER_REF_TYPE: refType,
    }));
    assert.equal(validated.event, event);
  }
});

test('rejects a workflow_call invocation from a caller that is not the Release workflow', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CALLER_WORKFLOW: 'CI' }), /not an allowed controller caller/);
});

test('rejects a workflow_call invocation with an inadmissible caller event', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CALLER_EVENT: 'issue_comment' }), /not a release event/);
});

test('rejects a workflow_call invocation whose caller SHA is not a full SHA', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CALLER_SHA: 'short' }), /caller SHA is not a full/);
});

test('rejects a workflow_call push caller ref that is not a release tag', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CALLER_REF: 'refs/heads/main', CALLER_REF_TYPE: 'branch' }), /not a v\*\.\*\.\* release tag/);
});

test('rejects a workflow_call schedule caller ref that is not the default branch', async () => {
  const api = releaseApi({ event: 'schedule', head_branch: 'refs/heads/main' });
  api.state.metadataZip = metadataArtifactZip({
    event: 'schedule', pr_number: null, label: null, head_repo: REPOSITORY, base_repo: REPOSITORY,
    event_base_sha: null, candidate_sha: HEAD_SHA, case: 'all',
  });
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'schedule', CALLER_REF: 'refs/heads/release-1.2', CALLER_REF_TYPE: 'branch',
  }), /not the default branch/);
});

test('rejects a workflow_call invocation whose source run head SHA differs from the caller SHA', async () => {
  // Run and metadata agree with each other (head ffff...), but the caller's
  // github.sha is a different commit: the build did not build the caller SHA.
  const api = releaseApi({ head_sha: 'f'.repeat(40) });
  api.state.metadataZip = metadataArtifactZip({
    event: 'push', pr_number: null, label: null, head_repo: REPOSITORY, base_repo: REPOSITORY,
    event_base_sha: null, head_sha: 'f'.repeat(40), candidate_sha: 'f'.repeat(40), case: 'all',
  });
  await expectRejected(api, callEnv(), /does not match the caller SHA/);
});

test('rejects a workflow_call input base SHA that differs from the validated build base SHA', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ INPUT_BASE_SHA: 'f'.repeat(40) }), /input base SHA .* does not match the validated build base SHA/);
});

test('rejects a workflow_call input candidate SHA that differs from the validated candidate SHA', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ INPUT_CANDIDATE_SHA: 'f'.repeat(40) }), /input candidate SHA .* does not match the validated candidate SHA/);
});

test('rejects a workflow_call input base artifact id that differs from the validated id', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ INPUT_BASE_ARTIFACT_ID: '999' }), /input base artifact id .* does not match the validated base artifact id/);
});

test('rejects a workflow_call input candidate artifact id that differs from the validated id', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ INPUT_CANDIDATE_ARTIFACT_ID: '999' }), /input candidate artifact id .* does not match the validated candidate artifact id/);
});

test('rejects a workflow_call input case that differs from the validated case', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ INPUT_CASE: 'heavy' }), /input case .* does not match the validated case/);
});

test('rejects a workflow_call invocation without caller context env', async () => {
  const api = releaseApi();
  await expectRejected(api, envFor({ SOURCE_MODE: 'workflow_call' }), /requires CALLER_WORKFLOW/);
});

// ---------------------------------------------------------------------------
// workflow_call: enclosing Release run shape (fix: same-Release-run semantics)
// ---------------------------------------------------------------------------

// workflow_dispatch caller fixture: the enclosing Release run triggered by
// workflow_dispatch, with matching untrusted metadata.
function releaseDispatchApi() {
  const api = releaseApi({ event: 'workflow_dispatch', head_branch: 'main' });
  api.state.metadataZip = metadataArtifactZip({
    event: 'workflow_dispatch', pr_number: null, label: null, head_repo: REPOSITORY, base_repo: REPOSITORY,
    event_base_sha: null, candidate_sha: HEAD_SHA, case: 'all',
  });
  return api;
}

test('accepts a workflow_call invocation whose source run is the in-progress enclosing Release run', async () => {
  const api = releaseApi();
  const validated = await expectAccepted(api, callEnv());
  // The enclosing run keeps the Release identity (not Query Regression) and
  // is still executing (in_progress, no conclusion) while the controller
  // runs; the admission does not require 'Query Regression' or success.
  assert.equal(validated.source_run_id, RUN_ID);
  assert.equal(validated.source_run_attempt, RUN_ATTEMPT);
  assert.equal(validated.repository, REPOSITORY);
  assert.equal(validated.event, 'push');
});

test('accepts a workflow_call workflow_dispatch caller on the default branch', async () => {
  const api = releaseDispatchApi();
  const validated = await expectAccepted(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/heads/main', CALLER_REF_TYPE: 'branch',
  }));
  assert.equal(validated.event, 'workflow_dispatch');
});

test('rejects a workflow_call invocation whose source run has already completed', async () => {
  const api = releaseApi({ status: 'completed', conclusion: 'success' });
  await expectRejected(api, callEnv(), /must still be in_progress/);
});

test('rejects a workflow_call invocation whose source run already has a conclusion', async () => {
  const api = releaseApi({ conclusion: 'success' });
  await expectRejected(api, callEnv(), /must not be concluded/);
});

test('rejects a workflow_call invocation whose source run is not the Release workflow', async () => {
  const api = releaseApi({ name: 'CI' });
  await expectRejected(api, callEnv(), /not the enclosing Release workflow/);
});

test('rejects a workflow_call invocation whose source run is not at the Release workflow path', async () => {
  const api = releaseApi({ path: '.github/workflows/evil.yml' });
  await expectRejected(api, callEnv(), /not the enclosing Release workflow/);
});

test('rejects a workflow_call invocation whose Release run path is a different workflow file (ref-qualified)', async () => {
  // A ref-qualified REST path to a different workflow file must fail closed:
  // only the exact release.yml path is the Release workflow identity.
  for (const path of [
    '.github/workflows/release-other.yml',
    '.github/workflows/release-other.yml@refs/tags/v1.2.3',
    '.github/workflows/query-regression.yml@refs/heads/main',
  ]) {
    const api = releaseApi({ path });
    await expectRejected(api, callEnv(), /not the enclosing Release workflow/);
  }
});

test('rejects a workflow_call invocation whose source run event is not a Release event', async () => {
  const api = releaseApi({ event: 'pull_request' });
  await expectRejected(api, callEnv(), /not a Release event/);
});

test('rejects a workflow_call invocation whose source run event differs from the caller event', async () => {
  // The API reports the enclosing run as workflow_dispatch while the
  // runner-provided caller context claims push: mismatched caller context.
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({ CALLER_EVENT: 'push' }), /does not match the caller event/);
});

test('rejects a workflow_call invocation whose controller run id differs from the source run id', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CONTROLLER_RUN_ID: String(RUN_ID + 1) }), /does not equal the source run id/);
});

test('rejects a workflow_call invocation whose controller run attempt differs from the source run attempt', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ CONTROLLER_RUN_ATTEMPT: '2' }), /does not equal the source run attempt/);
});

test('rejects a workflow_call invocation with a spoofed source run id', async () => {
  const api = releaseApi();
  await expectRejected(api, callEnv({ SOURCE_RUN_ID: String(RUN_ID + 1) }), /returned HTTP 404/);
});

test('rejects a workflow_call invocation with a spoofed source run attempt', async () => {
  const api = releaseApi({ run_attempt: 3 });
  await expectRejected(api, callEnv({ SOURCE_RUN_ATTEMPT: '1' }), /attempt 3 does not match/);
});

// ---------------------------------------------------------------------------
// workflow_call: workflow_dispatch ref validation (fix: exact two forms)
// ---------------------------------------------------------------------------

test('rejects a workflow_dispatch caller on an arbitrary (non-release) tag', async () => {
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/tags/arbitrary', CALLER_REF_TYPE: 'tag',
  }), /workflow_dispatch caller ref .* is not a release tag/);
});

test('rejects a workflow_dispatch caller with a malformed release tag ref', async () => {
  for (const ref of ['refs/tags/v1.2', 'refs/tags/v1.2.3-rc1', 'refs/tags/v1.2.3.4', 'v1.2.3']) {
    const api = releaseDispatchApi();
    await expectRejected(api, callEnv({
      CALLER_EVENT: 'workflow_dispatch', CALLER_REF: ref, CALLER_REF_TYPE: 'tag',
    }), /workflow_dispatch caller ref .* is not a release tag/);
  }
});

test('rejects a workflow_dispatch caller whose tag type is paired with a branch ref', async () => {
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/heads/main', CALLER_REF_TYPE: 'tag',
  }), /workflow_dispatch caller ref .* is not a release tag/);
});

test('rejects a workflow_dispatch caller whose branch type is paired with a tag ref', async () => {
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/tags/v1.2.3', CALLER_REF_TYPE: 'branch',
  }), /workflow_dispatch caller ref .* is not the default branch/);
});

test('rejects a workflow_dispatch caller on a non-default branch', async () => {
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/heads/feature/x', CALLER_REF_TYPE: 'branch',
  }), /workflow_dispatch caller ref .* is not the default branch/);
});

test('rejects a workflow_dispatch caller with a malformed (non-ref) ref', async () => {
  for (const ref of ['main', 'refs/main', 'refs/tags/', '']) {
    const api = releaseDispatchApi();
    await expectRejected(api, callEnv({
      CALLER_EVENT: 'workflow_dispatch', CALLER_REF: ref, CALLER_REF_TYPE: 'branch',
    }), /workflow_dispatch caller ref .* is not the default branch/);
  }
});

test('rejects a workflow_dispatch caller whose ref type is neither tag nor branch', async () => {
  const api = releaseDispatchApi();
  await expectRejected(api, callEnv({
    CALLER_EVENT: 'workflow_dispatch', CALLER_REF: 'refs/tags/v1.2.3', CALLER_REF_TYPE: 'other',
  }), /workflow_dispatch caller ref .* is neither a tag nor a branch/);
});

// ---------------------------------------------------------------------------
// Rejection paths
// ---------------------------------------------------------------------------

test('rejects a wrong repository in the run', async () => {
  const api = makeApi({ runOverrides: { repository: { full_name: 'other/repo' } } });
  await expectRejected(api, {}, /not .*GreptimeTeam\/greptimedb/);
});

test('rejects a run of a different workflow name', async () => {
  const api = makeApi({ runOverrides: { name: 'Some Other Workflow' } });
  await expectRejected(api, {}, /belongs to workflow/);
});

test('rejects a workflow_run build run whose path is not the Query Regression workflow file', async () => {
  // Exact canonical path is required: a different workflow file fails even
  // when the display name is correct (path is the canonical identity).
  for (const path of [
    '.github/workflows/evil.yml',
    '.github/workflows/evil.yml@refs/heads/main',
    '.github/workflows/query-regression-controller.yml@refs/heads/main',
  ]) {
    const api = makeApi({ runOverrides: { path } });
    await expectRejected(api, {}, /not the Query Regression workflow/);
  }
});

test('rejects a mismatched run id (replay of another run)', async () => {
  const api = makeApi();
  await expectRejected(api, { SOURCE_RUN_ID: String(RUN_ID + 1) }, /returned HTTP 404/);
});

test('rejects a mismatched run attempt (replay of an earlier attempt)', async () => {
  const api = makeApi({ runOverrides: { run_attempt: 3 } });
  await expectRejected(api, { SOURCE_RUN_ATTEMPT: '1' }, /attempt 3 does not match/);
});

test('rejects a failed or cancelled run conclusion', async () => {
  for (const conclusion of ['failure', 'cancelled', 'timed_out']) {
    const api = makeApi({ runOverrides: { conclusion } });
    await expectRejected(api, {}, /conclusion is/);
  }
});

test('rejects a run with an inadmissible event', async () => {
  const api = makeApi({ runOverrides: { event: 'issue_comment' } });
  await expectRejected(api, {}, /event .* is not admissible/);
});

test('rejects a run whose head SHA is not a full SHA', async () => {
  const api = makeApi({ runOverrides: { head_sha: 'short' } });
  await expectRejected(api, {}, /head_sha is not a full SHA/);
});

test('rejects a metadata run id that does not match the run (replay of a stale artifact)', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ run_id: RUN_ID - 1 });
  await expectRejected(api, {}, /possible replay/);
});

test('rejects a metadata attempt that does not match the run', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ run_attempt: 2 });
  await expectRejected(api, {}, /possible replay/);
});

test('rejects metadata from a different repository', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ repository: 'evil/repo' });
  await expectRejected(api, {}, /does not match/);
});

test('rejects a metadata head SHA that differs from the run head SHA', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ head_sha: 'd'.repeat(40) });
  await expectRejected(api, {}, /head SHA .* does not match run head SHA/);
});

test('rejects metadata whose base or candidate SHA is not a full SHA', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ built_base_sha: 'xyz' });
  await expectRejected(api, {}, /must be full SHAs/);
});

test('rejects an artifact id mismatch (base artifact id not from this run)', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ base_artifact_id: BASE_ARTIFACT_ID + 500 });
  await expectRejected(api, {}, /base artifact id .* does not match run artifact id/);
});

test('rejects an artifact id mismatch (candidate artifact id not from this run)', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ candidate_artifact_id: CANDIDATE_ARTIFACT_ID + 500 });
  await expectRejected(api, {}, /candidate artifact id .* does not match run artifact id/);
});

test('rejects when the metadata artifact is missing from the run', async () => {
  const api = makeApi({
    artifactOverrides: [{ id: 999, name: 'unrelated', expired: false }],
  });
  // Remove the metadata artifact from the listing.
  api.state.artifacts = api.state.artifacts.filter(a => a.name !== 'query-regression-metadata');
  await expectRejected(api, {}, /expected exactly one artifact named query-regression-metadata/);
});

test('rejects when the base artifact is missing from the run', async () => {
  const api = makeApi();
  api.state.artifacts = api.state.artifacts.filter(a => a.name !== 'query-regression-base-binaries');
  await expectRejected(api, {}, /expected exactly one artifact named query-regression-base-binaries/);
});

test('rejects when the candidate artifact is missing from the run', async () => {
  const api = makeApi();
  api.state.artifacts = api.state.artifacts.filter(a => a.name !== 'query-regression-candidate-binaries');
  await expectRejected(api, {}, /expected exactly one artifact named query-regression-candidate-binaries/);
});

test('rejects an expired artifact', async () => {
  const api = makeApi();
  api.state.artifacts = api.state.artifacts.map(a => (
    a.name === 'query-regression-metadata' ? { ...a, expired: true } : a
  ));
  await expectRejected(api, {}, /is expired/);
});

test('rejects an artifact listed twice', async () => {
  const api = makeApi();
  api.state.artifacts.push({ id: 777, name: 'query-regression-base-binaries', expired: false });
  await expectRejected(api, {}, /expected exactly one artifact named query-regression-base-binaries/);
});

test('rejects a PR number not listed in the run pull_requests', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ pr_number: 99 });
  await expectRejected(api, {}, /not listed in run/);
});

test('rejects a fork fallback with no matching open PR', async () => {
  const fork = 'octocat';
  const api = makeApi({
    runOverrides: {
      head_repository: { full_name: `${fork}/greptimedb`, owner: { login: fork }, name: REPO },
      pull_requests: [],
    },
  });
  api.state.pullsForHead = [];
  await expectRejected(api, {}, /matched 0 open PRs/);
});

test('rejects a fork fallback with multiple matching open PRs', async () => {
  const fork = 'octocat';
  const api = makeApi({
    runOverrides: {
      head_repository: { full_name: `${fork}/greptimedb`, owner: { login: fork }, name: REPO },
      pull_requests: [],
    },
  });
  api.state.pullsForHead = [
    { number: 42, head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } }, base: { repo: { full_name: REPOSITORY } } },
    { number: 43, head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } }, base: { repo: { full_name: REPOSITORY } } },
  ];
  await expectRejected(api, {}, /matched 2 open PRs/);
});

test('rejects a fork fallback that resolves to a different PR number', async () => {
  const fork = 'octocat';
  const api = makeApi({
    runOverrides: {
      head_repository: { full_name: `${fork}/greptimedb`, owner: { login: fork }, name: REPO },
      pull_requests: [],
    },
  });
  api.state.pullsForHead = [{
    number: 43,
    head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } },
    base: { repo: { full_name: REPOSITORY } },
  }];
  await expectRejected(api, {}, /does not match run PR/);
});

test('rejects a closed (stale) PR', async () => {
  const api = makeApi({ prOverrides: { state: 'closed' } });
  await expectRejected(api, {}, /is closed/);
});

test('rejects a stale PR whose head moved since the run', async () => {
  const api = makeApi({ prOverrides: { head: { sha: 'e'.repeat(40), repo: { full_name: REPOSITORY } } } });
  await expectRejected(api, {}, /current PR head SHA differs/);
});

test('rejects a moved PR whose head repo changed', async () => {
  const api = makeApi({
    prOverrides: { head: { sha: HEAD_SHA, repo: { full_name: 'other/greptimedb' } } },
  });
  await expectRejected(api, {}, /repository metadata does not match/);
});

test('rejects a PR with no regression label (missing label)', async () => {
  const api = makeApi({ prOverrides: { labels: [{ name: 'enhancement' }] } });
  await expectRejected(api, {}, /must carry exactly one regression label/);
});

test('rejects a PR with two regression labels', async () => {
  const api = makeApi({
    prOverrides: { labels: [{ name: 'query-regression' }, { name: 'heavy-regression' }] },
  });
  await expectRejected(api, {}, /must carry exactly one regression label/);
});

test('rejects metadata with a disallowed label', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ label: 'not-a-regression-label' });
  await expectRejected(api, {}, /not an allowed regression label/);
});

test('rejects a label mismatch between metadata and the current PR', async () => {
  const api = makeApi({ prOverrides: { labels: [{ name: 'heavy-regression' }] } });
  api.state.metadataZip = metadataArtifactZip({ label: 'query-regression' });
  await expectRejected(api, {}, /does not match the current PR label/);
});

test('rejects a metadata case that does not match the validated label', async () => {
  const api = makeApi({ prOverrides: { labels: [{ name: 'heavy-regression' }] } });
  api.state.metadataZip = metadataArtifactZip({ label: 'heavy-regression', case: 'all' });
  await expectRejected(api, {}, /does not match label heavy-regression/);
});

test('rejects a non-PR run with an empty metadata case', async () => {
  const api = makeApi({
    runOverrides: { event: 'workflow_dispatch', head_branch: 'main', pull_requests: [] },
  });
  api.state.metadataZip = metadataArtifactZip({
    event: 'workflow_dispatch', pr_number: null, label: null, case: '',
    head_repo: REPOSITORY, base_repo: REPOSITORY, event_base_sha: null, candidate_sha: HEAD_SHA,
  });
  await expectRejected(api, {}, /case must be a non-empty string/);
});

test('rejects a candidate merge SHA without exactly two parents', async () => {
  const api = makeApi();
  api.state.mergeParents = [HEAD_SHA];
  await expectRejected(api, {}, /has 1 parents; expected 2/);
});

test('rejects a candidate merge SHA with zero parents', async () => {
  const api = makeApi();
  api.state.mergeParents = [];
  await expectRejected(api, {}, /has 0 parents; expected 2/);
});

test('rejects a commit response whose sha differs from the requested candidate SHA', async () => {
  const api = makeApi();
  // The API returned a different object than the requested candidate SHA
  // (aliasing/redirect): the response top-level sha must equal the requested
  // candidate merge SHA exactly.
  api.state.commitShaOverride = 'f'.repeat(40);
  await expectRejected(api, {}, /commit API returned f+f+ for the requested candidate merge SHA/);
});

test('rejects a candidate merge SHA without the head parent', async () => {
  const api = makeApi();
  api.state.mergeParents = [BASE_SHA, 'd'.repeat(40)];
  await expectRejected(api, {}, /exactly one parent equal to head SHA/);
});

test('rejects a candidate merge SHA with two head parents', async () => {
  const api = makeApi();
  api.state.mergeParents = [HEAD_SHA, HEAD_SHA];
  await expectRejected(api, {}, /exactly one parent equal to head SHA/);
});

test('rejects a candidate merge SHA whose other parent is not the built base SHA', async () => {
  const api = makeApi();
  api.state.mergeParents = [HEAD_SHA, 'f'.repeat(40)];
  await expectRejected(api, {}, /exactly one parent equal to built base SHA/);
});

test('rejects metadata whose head repo differs from the run head repo', async () => {
  const fork = 'octocat';
  const api = makeApi({
    runOverrides: {
      head_repository: { full_name: `${fork}/greptimedb`, owner: { login: fork }, name: REPO },
      pull_requests: [],
    },
    prOverrides: {
      head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } },
    },
  });
  api.state.pullsForHead = [{
    number: 42,
    head: { sha: HEAD_SHA, repo: { full_name: `${fork}/greptimedb` } },
    base: { repo: { full_name: REPOSITORY } },
  }];
  api.state.metadataZip = metadataArtifactZip({ head_repo: REPOSITORY });
  await expectRejected(api, {}, /does not match run head repo/);
});

test('rejects invalid or missing environment', async () => {
  const api = makeApi();
  const { fetchImpl } = api;
  const validator = createValidator({ fetchImpl });
  await assert.rejects(
    () => validator.validate({ GITHUB_API_URL: 'https://api.github.com', GITHUB_TOKEN: 't' }),
    /GITHUB_REPOSITORY are required/
  );
  await assert.rejects(
    () => validator.validate({ GITHUB_API_URL: 'https://api.github.com', GITHUB_TOKEN: 't', GITHUB_REPOSITORY: REPOSITORY }),
    /SOURCE_RUN_ID must be a positive integer/
  );
  await assert.rejects(
    () => validator.validate(envFor({ SOURCE_RUN_ATTEMPT: '0' })),
    /SOURCE_RUN_ATTEMPT must be a positive integer/
  );
});

test('rejects a metadata artifact that is not valid JSON', async () => {
  const api = makeApi();
  api.state.metadataZip = zipOf({ 'query-regression-metadata.json': 'not json' });
  await expectRejected(api, {}, /not valid JSON/);
});

test('rejects an unsupported metadata schema version', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ schema_version: 2 });
  await expectRejected(api, {}, /schema_version 2 is not supported/);
});

test('rejects a run whose event is pull_request but metadata event is workflow_call', async () => {
  const api = makeApi();
  api.state.metadataZip = metadataArtifactZip({ event: 'workflow_call' });
  await expectRejected(api, {}, /does not match run event pull_request/);
});

test('rejects a run whose event is push but metadata event is pull_request', async () => {
  const api = makeApi({ runOverrides: { event: 'push', pull_requests: [] } });
  api.state.metadataZip = metadataArtifactZip({ event: 'pull_request', pr_number: null, label: null });
  await expectRejected(api, {}, /does not match run event push/);
});

test('exposes only the intended test seam and constants', () => {
  assert.equal(typeof admission.createValidator, 'function');
  assert.equal(typeof admission.runAdmission, 'function');
  assert.deepEqual(admission.ALLOWED_LABELS, ALLOWED_LABELS);
  assert.equal(admission.DEFAULT_SOURCE_WORKFLOW, 'Query Regression');
  assert.equal(admission.RELEASE_WORKFLOW_PATH, '.github/workflows/release.yml');
  assert.equal(admission.SOURCE_WORKFLOW_PATH, '.github/workflows/query-regression.yml');
  assert.equal(typeof admission.canonicalRunPath, 'function');
  assert.deepEqual(admission.ALLOWED_CALLER_WORKFLOWS, new Set(['Release']));
  assert.deepEqual(admission.ALLOWED_CALLER_EVENTS, new Set(['push', 'schedule', 'workflow_dispatch']));
  assert.equal(admission.RELEASE_TAG_REF_RE.source, '^refs\\/tags\\/v\\d+\\.\\d+\\.\\d+$');
});

test('canonicalizes REST run paths by stripping the first @ref suffix', () => {
  // Unqualified paths are unchanged; branch-, tag-, and SHA-qualified paths
  // reduce to the workflow file alone; non-string values pass through.
  assert.equal(admission.canonicalRunPath('.github/workflows/query-regression.yml'), '.github/workflows/query-regression.yml');
  assert.equal(admission.canonicalRunPath('.github/workflows/query-regression.yml@refs/heads/main'), '.github/workflows/query-regression.yml');
  assert.equal(admission.canonicalRunPath('.github/workflows/query-regression.yml@refs/tags/v1.2.3'), '.github/workflows/query-regression.yml');
  assert.equal(admission.canonicalRunPath(`.github/workflows/query-regression.yml@${HEAD_SHA}`), '.github/workflows/query-regression.yml');
  assert.equal(admission.canonicalRunPath('.github/workflows/release.yml@refs/heads/main@extra'), '.github/workflows/release.yml');
  assert.equal(admission.canonicalRunPath(null), null);
  assert.equal(admission.canonicalRunPath(undefined), undefined);
});
