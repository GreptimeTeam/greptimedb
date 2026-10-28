import assert from "node:assert/strict";
import test from "node:test";
import { isTeamMember, listAuthorOpenPrs, openPrLimit, shouldThrottleMatrix } from "./pr-open-limit-policy.js";

const context = {
  eventName: "pull_request",
  repo: { owner: "GreptimeTeam", repo: "greptimedb" },
  payload: { pull_request: { user: { login: "member" }, draft: false } },
};

function client({ permission = "write", count = 6, failure } = {}) {
  return {
    repos: {
      async getCollaboratorPermissionLevel(params) {
        assert.deepEqual(params, { ...context.repo, username: "member" });
        if (failure === "permission") throw { status: 403 };
        return { data: { permission } };
      },
    },
    pulls: { list: "pulls.list" },
    async paginate(method, params) {
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

test("shared listing excludes drafts and other authors across paginated results", async () => {
  const prs = await listAuthorOpenPrs(client({ count: 105 }), "GreptimeTeam", "greptimedb", "member");
  assert.equal(prs.length, 105);
  assert.ok(prs.every((pr) => pr.user.login === "member" && !pr.draft));
});

test("advisory retains its permission-error fallback", async () => {
  assert.equal(await isTeamMember(client({ failure: "permission" }), "GreptimeTeam", "greptimedb", "member"), true);
});

test("limit parser preserves the advisory configuration contract", () => {
  for (const value of [undefined, "invalid", "-1", "1.5"]) assert.equal(openPrLimit(value), 5);
  assert.equal(openPrLimit("0"), 0);
  assert.equal(openPrLimit("10"), 10);
});
