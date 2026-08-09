// Warns when a repository member has too many open pull requests.
//
// Review capacity is the bottleneck, so authors are expected to land or close
// existing work before opening more. Drafts count: they still occupy attention.
// This is advisory only — it never closes a pull request or fails the job.

import { appendFileSync } from "node:fs";

const MARKER = "<!-- pr-open-limit -->";
// Associations that identify an organization member. Outside collaborators and
// community contributors are out of scope.
const MEMBER_ASSOCIATIONS = ["OWNER", "MEMBER"];
// Keep the comment readable for authors who are far over the limit.
const MAX_LISTED_PRS = 10;

function summary(text) {
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);
  }
  console.log(text);
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
  const existing = comments.find((comment) => comment.body?.includes(MARKER));

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
  const association = process.env.PR_AUTHOR_ASSOCIATION;
  const limit = Number(process.env.MAX_OPEN_PRS || 5);

  if (author.endsWith("[bot]")) {
    summary(`Skipping bot author \`${author}\`.`);
    return;
  }

  if (!MEMBER_ASSOCIATIONS.includes(association)) {
    summary(`Skipping \`${author}\`: association is \`${association}\`, not an org member.`);
    return;
  }

  const octokit = new Octokit({ auth: process.env.GITHUB_TOKEN });

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
