# Nightly merge

The PR template opts new PRs into the `merge nightly` label. The Nightly Merge
workflow reconciles the label on PR creation, edits, and reopening. Existing PRs
without the checkbox can opt in by adding the label manually; label sync leaves
their labels unchanged. Unchecking removes the label; it does not
request an immediate merge or cancel an existing queue entry/auto-merge request.

Every day at 02:10, 03:10, 04:10, 05:10, 06:10, 07:10, and 08:10
Asia/Shanghai (UTC+8), the workflow enqueues up to five eligible PRs targeting
`main` per run, oldest first. Already queued PRs do not count toward this limit.
GitHub may delay scheduled runs.
The label alone controls nightly opt-in; the enqueue job does not check the PR
body. PRs must be approved and clean, and have all
review threads resolved. Drafts are skipped.
Already queued PRs and manually enabled auto-merge requests are left alone;
Maintainers can use **Merge when ready** to proceed earlier. Unready PRs wait for a later
scheduled run; this workflow never enables auto-merge on their behalf.

## Repository setup

- Create the `merge nightly` label before enabling the workflow.
- Require the merge queue, reviews, status checks, and conversation resolution
  on `main`. Conversation resolution must also be enforced by GitHub to cover
  new comments arriving between the script's check and enqueueing.
- Ensure the existing `GH_PERSONAL_ACCESS_TOKEN` automation secret can read PRs
  and review threads and enqueue PRs in this repository. Do not grant it a
  branch-rule bypass. This credential is already referenced by other workflows;
  its actual permissions have not been validated by the local tests.
- Run **Nightly Merge** manually with the default `dry_run: true` first and
  inspect its job summary. Setting it to false explicitly enqueues outside the
  scheduled time as well. Merge queue build concurrency is configured separately
  in GitHub; this workflow does not change it.

The workflow never checks out or executes PR code. Label reconciliation uses
`GITHUB_TOKEN`; enqueueing uses the automation credential so downstream CI can
run. No runner remains allocated while waiting for the nightly schedule.

## Local validation

Run `actionlint .github/workflows/nightly-merge.yml .github/workflows/checks.yml`
and `node --test .github/scripts/tests/nightly-merge.test.cjs`.
The tests execute the actual inline workflow scripts with mocked GitHub APIs,
including one-factor comparisons against an eligible PR. They do not validate
live GitHub permissions, branch rules, or runner savings.
