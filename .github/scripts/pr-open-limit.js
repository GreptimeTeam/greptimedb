// Warns when a repository member has too many pending pull requests.
//
// Review capacity is the bottleneck, so authors are expected to land or close
// existing work before opening more. Drafts and approved PRs are not counted:
// they are not waiting for review.
// This is advisory only — it never closes a pull request or fails the job.

import { appendFileSync } from "node:fs";
import { isTeamMember, listAuthorPendingPrs, openPrLimit } from "./pr-open-limit-policy.js";

const MARKER = "<!-- pr-open-limit -->";
// GITHUB_TOKEN can only edit comments authored by the Actions bot itself.
const COMMENT_AUTHOR = "github-actions[bot]";
// Keep the comment readable for authors who are far over the limit.
const MAX_LISTED_PRS = 10;

function summary(text) {
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${text}\n`);
  }
  console.log(text);
}

function buildComment(author, total, limit, owner, repo, pendingPrs) {
  const listed = pendingPrs.slice(0, MAX_LISTED_PRS).map((pr) => {
    // Escape ASCII punctuation so titles stay literal Markdown link labels.
    const title = pr.title
      .replace(/\s+/g, " ")
      .replace(/[\x21-\x2f\x3a-\x40\x5b-\x60\x7b-\x7e]/g, "\\$&");
    // Redirect links avoid creating backlinks on the listed PRs.
    return `- [PR ${pr.number}: ${title}](https://redirect.github.com/${owner}/${repo}/pull/${pr.number})`;
  });
  const hidden = pendingPrs.length - listed.length;
  if (hidden > 0) {
    listed.push(`- ...and ${hidden} more`);
  }
  const list = listed.length > 0 ? `${listed.join("\n")}\n\n` : "";
  const query = `is:pr is:open author:${author} draft:false`;
  const url = `https://github.com/${owner}/${repo}/pulls?q=${encodeURIComponent(query)}`;

  return `${MARKER}
> [!WARNING]
> @${author} has **${total}** pending pull requests (open, non-draft, not approved) in this repository, over the limit of **${limit}**.

Review is the scarcest resource here. Please land or close some of these before
pushing this one forward.

${list}[View all your open non-draft pull requests](${url})

This reminder does not block merging. Integration CI reduces matrix parallelism
to 1 for team members over the limit when a new PR run starts.`;
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
  const limit = openPrLimit(process.env.MAX_OPEN_PRS);

  if (author.endsWith("[bot]")) {
    summary(`Skipping bot author \`${author}\`.`);
    return;
  }

  if (process.env.PR_IS_DRAFT === "true") {
    summary(`Skipping #${prNumber}: draft.`);
    return;
  }

  const octokit = new Octokit({ auth: process.env.GITHUB_TOKEN });

  if (!(await isTeamMember(octokit, owner, repo, author))) {
    summary(`Skipping \`${author}\`: not a team member.`);
    return;
  }

  const pending = await listAuthorPendingPrs(octokit, owner, repo, author);
  const total = pending.length;

  if (total <= limit) {
    summary(`\`${author}\` has ${total} pending pull requests (limit ${limit}). OK.`);
    return;
  }

  summary(`\`${author}\` has ${total} pending pull requests, over the limit of ${limit}.`);
  await upsertComment(
    octokit,
    { owner, repo, issue_number: prNumber },
    buildComment(author, total, limit, owner, repo, pending)
  );
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
