// Shared policy for the advisory and CI matrix throttling.
const TEAM_PERMISSIONS = ["admin", "maintain", "write"];

export function openPrLimit(value) {
  const limit = Number(value);
  return Number.isInteger(limit) && limit >= 0 ? limit : 5;
}

// `author_association` cannot be used here: it is computed from what the caller
// is allowed to see, so GITHUB_TOKEN reports a private organization member as
// CONTRIBUTOR. Repository permission is viewer-independent.
//
// The advisory defaults to applying the limit on errors. CI throttling opts
// out on errors so an unknown identity never slows an external contributor.
export async function isTeamMember(octokit, owner, repo, username, onError = true) {
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
      `Cannot read repository permission for \`${username}\` (HTTP ${error.status}); using the caller's fallback.`
    );
    return onError;
  }
}

// REST returns reviews in chronological order. Comments and unsubmitted
// reviews do not replace a reviewer's last approval/change-request decision.
export function hasEffectiveApproval(reviews) {
  const decisions = new Map();
  for (const review of reviews) {
    if (review.user && ["APPROVED", "CHANGES_REQUESTED", "DISMISSED"].includes(review.state)) {
      decisions.set(review.user.id, review.state);
    }
  }
  return [...decisions.values()].includes("APPROVED");
}

// Returns the author's open PRs counted toward the limit, excluding drafts
// and PRs with an effective approval.
export async function listAuthorOpenPrs(octokit, owner, repo, author) {
  const prs = await octokit.paginate(octokit.pulls.list, {
    owner, repo, state: "open", sort: "created", direction: "asc", per_page: 100,
  });
  const openPrs = [];
  for (const pr of prs) {
    if (pr.user?.login !== author || pr.draft) continue;
    const reviews = await octokit.paginate(octokit.pulls.listReviews, {
      owner, repo, pull_number: pr.number, per_page: 100,
    });
    if (!hasEffectiveApproval(reviews)) openPrs.push(pr);
  }
  return openPrs;
}

// Called with the read-only github-script client. Only a positive decision
// throttles; absent outputs and API failures preserve normal parallelism.
export async function shouldThrottleMatrix(octokit, context, rawLimit) {
  const pr = context.payload.pull_request;
  if (context.eventName !== "pull_request" || !pr || pr.draft ||
      pr.user.login.endsWith("[bot]")) {
    return false;
  }
  const { owner, repo } = context.repo;
  try {
    if (!(await isTeamMember(octokit, owner, repo, pr.user.login, false))) {
      return false;
    }
    const prs = await listAuthorOpenPrs(octokit, owner, repo, pr.user.login);
    return prs.length > openPrLimit(rawLimit);
  } catch (error) {
    console.log(`Cannot count open pull requests (HTTP ${error.status}); preserving matrix parallelism.`);
    return false;
  }
}
