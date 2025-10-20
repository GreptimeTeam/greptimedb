// Daily PR Review Reminder Script
// Fetches open PRs from GreptimeDB repository and sends Slack notifications
// to PR owners and assigned reviewers to keep review process moving.

(async () => {
  const { Octokit } = await import("@octokit/rest");
  const { default: axios } = await import('axios');

  // Configuration
  const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
  const SLACK_WEBHOOK_URL = process.env.SLACK_PR_REVIEW_WEBHOOK_URL;
  const REPO_OWNER = "GreptimeTeam";
  const REPO_NAME = "greptimedb";
  const GITHUB_TO_SLACK = JSON.parse(process.env.GITHUBID_SLACKID_MAPPING || '{}');

  // Debug: Print environment variable status
  console.log("=== Environment Variables Debug ===");
  console.log(`GITHUB_TOKEN: ${GITHUB_TOKEN ? 'Set ✓' : 'NOT SET ✗'}`);
  console.log(`SLACK_PR_REVIEW_WEBHOOK_URL: ${SLACK_WEBHOOK_URL ? 'Set ✓' : 'NOT SET ✗'}`);
  console.log(`GITHUBID_SLACKID_MAPPING: ${process.env.GITHUBID_SLACKID_MAPPING ? `Set ✓ (${Object.keys(GITHUB_TO_SLACK).length} mappings)` : 'NOT SET ✗'}`);
  console.log("===================================\n");

  const octokit = new Octokit({
    auth: GITHUB_TOKEN
  });

  // Fetch all open PRs from the repository
  async function fetchOpenPRs() {
    try {
      const prs = await octokit.pulls.list({
        owner: REPO_OWNER,
        repo: REPO_NAME,
        state: "open",
        per_page: 100,
        sort: "created",
        direction: "asc"
      });
      return prs.data.filter((pr) => !pr.draft);
    } catch (error) {
      console.error("Error fetching PRs:", error);
      return [];
    }
  }

  // Convert GitHub username to Slack mention or fallback to GitHub username
  function toSlackMention(githubUser) {
    const slackUserId = GITHUB_TO_SLACK[githubUser];
    return slackUserId ? `<@${slackUserId}>` : `@${githubUser}`;
  }

  // Calculate days since PR was opened
  function getDaysOpen(createdAt) {
    const created = new Date(createdAt);
    const now = new Date();
    const diffMs = now - created;
    const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    return days;
  }

  // Get urgency emoji based on PR age
  function getAgeEmoji(days) {
    if (days >= 14) return "🔴"; // 14+ days - critical
    if (days >= 7) return "🟠";  // 7+ days - urgent
    if (days >= 3) return "🟡";  // 3+ days - needs attention
    return "🟢"; // < 3 days - fresh
  }

  // Build Slack notification message from PR list
  function buildSlackMessage(prs) {
    if (prs.length === 0) {
      return "*🎉 Great job! No pending PRs for review.*";
    }

    const lines = [
      `*🔍 Daily PR Review Reminder 🔍*`,
      `Found *${prs.length}* open PR(s) waiting for review:\n`
    ];

    prs.forEach((pr, index) => {
      const owner = toSlackMention(pr.user.login);
      const reviewers = pr.requested_reviewers || [];
      const reviewerMentions = reviewers.map(r => toSlackMention(r.login)).join(", ");
      const daysOpen = getDaysOpen(pr.created_at);
      const ageEmoji = getAgeEmoji(daysOpen);

      const prInfo = `${index + 1}. <${pr.html_url}|#${pr.number}: ${pr.title}>`;
      const ageInfo = `   ${ageEmoji} Opened *${daysOpen}* day(s) ago`;
      const ownerInfo = `   👤 Owner: ${owner}`;
      const reviewerInfo = reviewers.length > 0
        ? `   👁️ Reviewers: ${reviewerMentions}`
        : `   👁️ Reviewers: _Not assigned yet_`;

      lines.push(prInfo);
      lines.push(ageInfo);
      lines.push(ownerInfo);
      lines.push(reviewerInfo);
      lines.push(""); // Empty line between PRs
    });

    lines.push("_🟢 < 3 days | 🟡 3-6 days | 🟠 7-13 days | 🔴 14+ days_");
    lines.push("_Let's keep the code review process moving! 🚀_");

    return lines.join("\n");
  }

  // Send notification to Slack webhook
  async function sendSlackNotification(message) {
    if (!SLACK_WEBHOOK_URL) {
      console.log("⚠️  SLACK_PR_REVIEW_WEBHOOK_URL not configured. Message preview:");
      console.log("=".repeat(60));
      console.log(message);
      console.log("=".repeat(60));
      return;
    }

    try {
      const response = await axios.post(SLACK_WEBHOOK_URL, {
        text: message
      });

      if (response.status !== 200) {
        throw new Error(`Slack API returned status ${response.status}`);
      }
      console.log("Slack notification sent successfully.");
    } catch (error) {
      console.error("Error sending Slack notification:", error);
      throw error;
    }
  }

  // Main execution flow
  async function run() {
    console.log(`Fetching open PRs from ${REPO_OWNER}/${REPO_NAME}...`);
    const prs = await fetchOpenPRs();
    console.log(`Found ${prs.length} open PR(s).`);

    const message = buildSlackMessage(prs);
    console.log("Sending Slack notification...");
    await sendSlackNotification(message);
  }

  run().catch(error => {
    console.error("Script execution failed:", error);
    process.exit(1);
  });
})();
