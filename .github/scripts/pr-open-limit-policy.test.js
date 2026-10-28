import assert from "node:assert/strict";
import test from "node:test";
import { hasEffectiveApproval, isTeamMember, listAuthorPendingPrs, openPrLimit, shouldThrottleMatrix } from "./pr-open-limit-policy.js";

const context = {
  eventName: "pull_request",
  repo: { owner: "GreptimeTeam", repo: "greptimedb" },
  payload: { pull_request: { user: { login: "member" }, draft: false } },
};

const review = (state, id = 1) => ({ state, user: { id } });

function client({ permission = "write", count = 6, approved = 0, failure } = {}) {
  return {
    repos: {
      async getCollaboratorPermissionLevel(params) {
        assert.deepEqual(params, { ...context.repo, username: "member" });
        if (failure === "permission") throw { status: 403 };
        return { data: { permission } };
      },
    },
    pulls: { list: "pulls.list", listReviews: "pulls.listReviews" },
    async paginate(method, params) {
      if (method === "pulls.listReviews") {
        assert.deepEqual(params, { ...context.repo, pull_number: params.pull_number, per_page: 100 });
        assert.ok(params.pull_number < count, "only query this author's non-draft PRs");
        if (failure === "reviews") throw { status: 502 };
        return params.pull_number < approved ? [review("APPROVED")] : [];
      }
      assert.equal(method, "pulls.list");
      assert.deepEqual(params, {
        ...context.repo, state: "open", sort: "created", direction: "asc", per_page: 100,
      });
      if (failure === "list") throw { status: 429 };
      return [
        ...Array.from({ length: count }, (_, number) => ({ number, user: { login: "member" }, draft: false })),
        { number: 999, user: { login: "member" }, draft: true },
        { number: 1000, user: { login: "external" }, draft: false },
      ];
    },
  };
}

for (const [name, options, limit, expected] of [
  ["at the default limit", { count: 5 }, undefined, false],
  ["approved PR frees a slot", { count: 6, approved: 1 }, "5", false],
  ["all approved", { count: 6, approved: 6 }, "0", false],
  ["still over after excluding approvals", { count: 7, approved: 1 }, "5", true],
  ["above the default limit", { count: 6 }, undefined, true],
  ["below the limit", { count: 4 }, "5", false],
  ["custom limit", { count: 6 }, "6", false],
  ["zero limit", { count: 1 }, "0", true],
  ["invalid limit", { count: 6 }, "invalid", true],
  ["maintainer", { permission: "maintain" }, "5", true],
  ["admin", { permission: "admin" }, "5", true],
  ["external contributor", { permission: "read" }, "5", false],
  ["triage", { permission: "triage" }, "5", false],
  ["no access", { permission: "none" }, "5", false],
  ["permission failure", { failure: "permission" }, "5", false],
  ["review lookup failure", { failure: "reviews" }, "5", false],
  ["listing failure", { failure: "list" }, "5", false],
]) {
  test(name, async () => {
    assert.equal(await shouldThrottleMatrix(client(options), context, limit), expected);
  });
}

for (const eventName of ["merge_group", "schedule", "workflow_dispatch", "push"]) {
  test(`${eventName} does not query the API`, async () => {
    assert.equal(await shouldThrottleMatrix({}, { ...context, eventName }, "5"), false);
  });
}
for (const [name, payload] of [
  ["missing PR", {}],
  ["draft", { pull_request: { ...context.payload.pull_request, draft: true } }],
  ["bot", { pull_request: { user: { login: "dependabot[bot]" } } }],
]) {
  test(`${name} does not query the API`, async () => {
    assert.equal(await shouldThrottleMatrix({}, { ...context, payload }, "5"), false);
  });
}

for (const [name, reviews, expected] of [
  ["no reviews", [], false],
  ["approval", [review("APPROVED")], true],
  ["comment after approval", [review("APPROVED"), review("COMMENTED")], true],
  ["pending review after approval", [review("APPROVED"), review("PENDING")], true],
  ["dismissed approval", [review("DISMISSED")], false],
  ["request changes after approval", [review("APPROVED"), review("CHANGES_REQUESTED")], false],
  ["approval after request changes", [review("CHANGES_REQUESTED"), review("APPROVED")], true],
  ["another reviewer's changes", [review("APPROVED", 1), review("CHANGES_REQUESTED", 2)], true],
  ["another approval survives dismissal", [review("APPROVED", 1), review("DISMISSED", 2)], true],
  ["later dismissal supersedes old approval", [review("APPROVED"), review("DISMISSED")], false],
  ["comment alone", [review("COMMENTED")], false],
  ["deleted reviewer", [{ state: "APPROVED", user: null }], false],
]) {
  test(name, () => assert.equal(hasEffectiveApproval(reviews), expected));
}

test("listing excludes approved PRs, drafts and other authors", async () => {
  const prs = await listAuthorPendingPrs(client({ count: 105, approved: 2 }), "GreptimeTeam", "greptimedb", "member");
  assert.equal(prs.length, 103);
  assert.deepEqual(prs.map((pr) => pr.number), Array.from({ length: 103 }, (_, i) => i + 2));
});

test("approval and dismissal change the pending count on the next run", async () => {
  const api = client();
  const original = api.paginate;
  let reviews = [];
  api.paginate = async (method, params) =>
    method === "pulls.listReviews" && params.pull_number === 0 ? reviews : original(method, params);
  for (const [next, expected] of [[[], true], [[review("APPROVED")], false],
      [[review("DISMISSED")], true], [[review("APPROVED")], false]]) {
    reviews = next;
    assert.equal(await shouldThrottleMatrix(api, context, "5"), expected);
  }
});

test("advisory retains its permission-error fallback", async () => {
  assert.equal(await isTeamMember(client({ failure: "permission" }), "GreptimeTeam", "greptimedb", "member"), true);
});

test("limit parser preserves the advisory configuration contract", () => {
  for (const value of [undefined, "invalid", "-1", "1.5"]) assert.equal(openPrLimit(value), 5);
  assert.equal(openPrLimit("0"), 0);
  assert.equal(openPrLimit("10"), 10);
});

// Exercise the executable advisory with a local Octokit stand-in; no API writes.
for (const [name, approved, expectedComments] of [
  ["approved triggering PR is not added back to the count", 1, 0],
  ["six pending PRs produce an accurate reminder", 0, 1],
]) {
  test(name, async () => {
    const { mkdtempSync, mkdirSync, copyFileSync, writeFileSync, readFileSync, rmSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");
    const { spawnSync } = await import("node:child_process");
    const dir = mkdtempSync(join(tmpdir(), "pr-limit-advisory-"));
    try {
      for (const file of ["pr-open-limit.js", "pr-open-limit-policy.js"]) {
        copyFileSync(new URL(file, import.meta.url), join(dir, file));
      }
      writeFileSync(join(dir, "package.json"), '{"type":"module"}');
      const moduleDir = join(dir, "node_modules/@octokit/rest");
      mkdirSync(moduleDir, { recursive: true });
      writeFileSync(join(moduleDir, "package.json"), '{"type":"module","exports":"./index.js"}');
      writeFileSync(join(moduleDir, "index.js"), `
        import { appendFileSync } from 'node:fs';
        export class Octokit {
          repos = { getCollaboratorPermissionLevel: async () => ({data: {permission: 'write'}}) };
          pulls = { list: 'pulls', listReviews: 'reviews' };
          issues = { listComments: 'comments', createComment: async (data) => {
            appendFileSync(process.env.COMMENTS_FILE, JSON.stringify(data) + '\\n');
          } };
          async paginate(method, params) {
            if (method === 'pulls') return Array.from({length: 6}, (_, number) => ({
              number, title: 'PR ' + number, user: {login: 'member'}, draft: false
            }));
            if (method === 'reviews') return params.pull_number < Number(process.env.APPROVED)
              ? [{state: 'APPROVED', user: {id: 1}}] : [];
            if (method === 'comments') return [];
            throw new Error('Unexpected endpoint');
          }
        }
      `);
      const commentsFile = join(dir, "comments.jsonl");
      writeFileSync(commentsFile, "");
      const result = spawnSync(process.execPath, [join(dir, "pr-open-limit.js")], {
        encoding: "utf8",
        env: { ...process.env, GITHUB_REPOSITORY: "GreptimeTeam/greptimedb",
          PR_NUMBER: "0", PR_AUTHOR: "member", PR_IS_DRAFT: "false", MAX_OPEN_PRS: "5",
          GITHUB_STEP_SUMMARY: join(dir, "summary.md"), APPROVED: String(approved), COMMENTS_FILE: commentsFile },
      });
      assert.equal(result.status, 0, result.stderr);
      const comments = readFileSync(commentsFile, "utf8").trim().split("\n").filter(Boolean).map(JSON.parse);
      assert.equal(comments.length, expectedComments);
      assert.match(readFileSync(join(dir, "summary.md"), "utf8"), new RegExp(`${6 - approved} pending pull requests`));
      if (expectedComments) {
        assert.match(comments[0].body, /\*\*6\*\* pending pull requests/);
        assert.equal(comments[0].issue_number, 0);
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
}
