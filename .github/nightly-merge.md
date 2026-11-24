# Nightly merge

Open PRs targeting `main` participate by default, including automated PRs
without a checklist. Check **Skip nightly merge** to opt out. The scheduler
honors the checkbox immediately, and label sync adds `skip nightly merge`.
Uncheck the box to release the bot's hold. Maintainers can also apply the label
directly and remove it when ready. If someone removes the label while the box
is checked, label sync restores it. A manually applied label survives title
and description edits.

Every day at 02:10, 03:10, 04:10, 05:10, 06:10, 07:10, and 08:10
Asia/Shanghai (UTC+8), the workflow enqueues up to five eligible PRs targeting
`main` per run, oldest first. Already queued PRs do not count toward this limit.
GitHub may delay scheduled runs.
PRs must be approved and clean, with all review threads resolved. Drafts are
skipped. The workflow rechecks the current opt-out label and checkbox before
enqueueing at the reviewed head SHA.
If GitHub rejects enqueueing because a PR belongs to a stack, the workflow skips
it and continues without failing the run or counting it toward the five-PR limit.
Other API errors still fail the run. Dry runs do not call the enqueue API, so
they cannot detect this stack rejection.
Already queued PRs and manually enabled auto-merge requests are left alone;
Maintainers can use **Merge when ready** to proceed earlier. Unready PRs wait for a later
scheduled run; this workflow never enables auto-merge on their behalf.

## Repository setup

- Create the `skip nightly merge` label before enabling the workflow.
- Require the merge queue, reviews, status checks, and conversation resolution
  on `main`. Conversation resolution must also be enforced by GitHub to cover
  new comments arriving between the script's check and enqueueing.
- Ensure the existing `GH_PERSONAL_ACCESS_TOKEN` automation secret can read PRs
  and review threads and enqueue PRs in this repository. Do not grant it a
  branch-rule bypass. This credential is already referenced by other workflows;
  its actual permissions have not been validated by the local tests.
- Run **Nightly Merge** manually with the default `dry_run: true` first and
  inspect its job summary. The repository variable `NIGHTLY_MERGE_ENABLED`
  must be `true` for scheduled runs and manual dispatches that enqueue PRs.
  Set it after completing the migration below.

The PR event job reads the checklist helper from the trusted workflow commit.
Label reconciliation uses `GITHUB_TOKEN`. Enqueueing uses the automation
credential so downstream CI can run.

## One-time migration of open PRs

Use a maintainer's authenticated `gh` session from the checkout containing
this change. PR body edits made by this account trigger checkbox label sync.

1. Before merging, create the opt-out label. Leave `NIGHTLY_MERGE_ENABLED`
   unset so the new workflow starts with enqueueing paused. Dry runs and label
   sync remain available. Let any active enqueue job from the old workflow finish.
2. Immediately after merging, capture the open PRs with their old labels.
   Supply the rollout PR number so the snapshot excludes PRs opened at or
   after GitHub's recorded merge time. New PRs can open throughout the rollout.
   Finish or rerun any pending label updates from the old workflow before
   taking the snapshot. Keep the file for verification and retries.

   ```sh
   node .github/scripts/migrate-pr-labels.cjs snapshot /tmp/pr-labels.json ROLLOUT_PR_NUMBER
   ```

3. Apply the saved snapshot. The migration removes `docs-not-required` from
   captured open PRs on every base branch. For PRs targeting `main` both in
   the snapshot and at apply time, it also converts legacy checkboxes and
   removes `merge nightly`. A converted checkbox is unchecked if the PR had
   `merge nightly` in the snapshot and checked otherwise. For PRs that lacked
   the old label, `apply` adds a manual hold only when the checkbox is missing.
   If an author unchecks **Skip nightly merge**, retrying `apply` honors that
   choice. Coordinate retargeting and description edits during apply because
   GitHub accepts the complete PR body on update.

   ```sh
   node .github/scripts/migrate-pr-labels.cjs apply /tmp/pr-labels.json
   ```

4. Let checkbox sync finish, then verify against the same snapshot. Verification
   lists outstanding changes and exits unsuccessfully for any mismatch. If a
   checked box still needs a label, rerun its failed label job or edit its
   description to trigger sync. Confirm any new manual opt-out with the
   maintainer and record it as an intentional difference. For a PR retargeted
   into `main`, record its current choice with a **Skip nightly merge** checkbox
   (checked to hold, unchecked to participate) or a manual `skip nightly merge`
   label. Verification flags a retargeted PR until it has a current choice.
   An interrupted apply can be rerun with the original snapshot.

   ```sh
   node .github/scripts/migrate-pr-labels.cjs verify /tmp/pr-labels.json
   ```

5. Run **Nightly Merge** with `dry_run: true` and inspect its summary. Once the
   verified pool is correct, set `NIGHTLY_MERGE_ENABLED` to `true` to resume enqueueing.

Docbot now adds `docs-required` when requested. On merge, that label controls
whether it creates a follow-up issue in the docs repository.

## Local validation

Run `actionlint .github/workflows/{nightly-merge,docbot,checks}.yml`,
`node --test .github/scripts/tests/nightly-merge.test.cjs`, and `pnpm --dir cyborg test`.
The tests execute the workflow scripts with mocked GitHub APIs,
including one-factor comparisons against an eligible PR. They do not validate
live GitHub permissions, branch rules, or runner savings.
