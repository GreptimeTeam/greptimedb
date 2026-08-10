// Warns when a repository member has too many open pull requests.
//
// Review capacity is the bottleneck, so authors are expected to land or close
// existing work before opening more. Drafts count: they still occupy attention.
// This is advisory only — it never closes a pull request or fails the job.

import { appendFileSync } from "node:fs";

const MARKER = "<!-- pr-open-limit -->";
// GITHUB_TOKEN can only edit comments authored by the Actions bot itself.
const COMMENT_AUTHOR = "github-actions[bot]";
// Repository permissions that mark someone as part of the team.
const TEAM_PERMISSIONS = ["admin", "maintain", "write"];
// Keep the comment readable for authors who are far over the limit.
const MAX_LISTED_PRS = 10;

function summary(text) {
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);
  }
  console.log(text);
}

// `author_association` cannot be used here: it is computed from what the caller
// is allowed to see, so GITHUB_TOKEN reports a private organization member as
// CONTRIBUTOR. Repository permission is viewer-independent.
//
// On error this returns true rather than false. A token that cannot read
// permissions would otherwise make the whole check silently pass everyone.
async function isTeamMember(octokit, owner, repo, username) {
  try {
    const { data } = await octokit.repos.getCollaboratorPermissionLevel({
      owner,
      repo,
      username,
    });
    // Never log the level itself: job logs are public.
    return TEAM_PERMISSIONS.includes(data.permission);
  } catch (error) {
    console.log(
      `Cannot read repository permission for \`${username}\` (HTTP ${error.status}); applying the limit anyway.`
    );
    return true;
  }
}

function buildComment(author, total, limit, openPrs) {
  const listed = openPrs
    .slice(0, MAX_LISTED_PRS)
    .map((pr) => `- #${pr.number} ${pr.title}${pr.draft ? " _(draft)_" : ""}`);
  const hidden = openPrs.length - listed.length;
  const list = hidden > 0 ? `${listed.join("\n")}\n- ...and ${hidden} more` : listed.join("\n");

  return `${MARKER}
> [!WARNING]
> @${author} has **${total}** open pull requests in this repository, over the limit of **${limit}**.

Review is the scarcest resource here. Please land or close some of these before
pushing this one forward:

${list}

This check is advisory for now and blocks nothing.`;
}

async function upsertComment(octokit, params, body) {
  const comments = await octokit.paginate(octokit.issues.listComments, {
    ...params,
    per_page: 100,
  });
  const existing = comments.find(
    (comment) =>
      comment.user?.login === COMMENT_AUTHOR && comment.body?.includes(MARKER)
  );

  if (existing) {
    await octokit.issues.updateComment({
      owner: params.owner,
      repo: params.repo,
      comment_id: existing.id,
      body,
    });
  } else {
    await octokit.issues.createComment({ ...params, body });
  }
}

(async () => {
  const { Octokit } = await import("@octokit/rest");

  const [owner, repo] = process.env.GITHUB_REPOSITORY.split("/");
  const prNumber = Number(process.env.PR_NUMBER);
  const author = process.env.PR_AUTHOR;
  const rawLimit = Number(process.env.MAX_OPEN_PRS);
  const limit = Number.isInteger(rawLimit) && rawLimit >= 0 ? rawLimit : 5;

  if (author.endsWith("[bot]")) {
    summary(`Skipping bot author \`${author}\`.`);
    return;
  }

  const octokit = new Octokit({ auth: process.env.GITHUB_TOKEN });

  if (!(await isTeamMember(octokit, owner, repo, author))) {
    summary(`Skipping \`${author}\`: not a team member.`);
    return;
  }

  const allOpen = await octokit.paginate(octokit.pulls.list, {
    owner,
    repo,
    state: "open",
    sort: "created",
    direction: "asc",
    per_page: 100,
  });
  // The pull request that triggered this run is already open, so exclude it and
  // report the total separately.
  const others = allOpen.filter(
    (pr) => pr.user?.login === author && pr.number !== prNumber
  );
  const total = others.length + 1;

  if (others.length < limit) {
    summary(`\`${author}\` has ${total} open pull requests (limit ${limit}). OK.`);
    return;
  }

  summary(`\`${author}\` has ${total} open pull requests, over the limit of ${limit}.`);
  await upsertComment(
    octokit,
    { owner, repo, issue_number: prNumber },
    buildComment(author, total, limit, others)
  );
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
