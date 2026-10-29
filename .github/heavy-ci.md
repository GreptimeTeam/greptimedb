# Heavy CI scheduling

Checks, Rust CI, and Integration CI each run the shared GitHub script test
action before expensive builds and tests. A failed, skipped, or cancelled
script-test job blocks those jobs, including manually dispatched runs. Each
workflow runs its own preflight so it does not wait on another workflow run.

Rust CI runs Windows and RISC-V compilation checks only on schedule or manual
dispatch, not on PRs or merge groups. Windows nextest and Windows SQLness run in the existing Nightly CI workflow, which also
supports manual dispatch. Its existing schedule is Tuesday through Friday at
07:00 Asia/Shanghai, not every day. No new Windows test job is added.

Integration CI admits fuzz when a non-draft PR is opened or marked ready for
review, selecting suites with the existing diff-scope classifier. Its weekday
schedule (23:00 Asia/Shanghai, Monday through Friday) runs all fuzz suites. Ordinary pushes and reopening do not run fuzz; core
integration checks still run. Merge groups continue to skip fuzz.

Re-running an ordinary push-triggered Integration CI run does not enable fuzz.
A new push can cancel an earlier ready-for-review run. Use an explicit fuzz
profile through the CI entry points below when later changes need fuzz coverage.

For a same-repository branch, use **Run workflow** and select `fuzz_profile`
(`standalone`, `distributed`, `chaos`, or `all`). The default `none` runs core
integration without fuzz. The existing `/ci fuzz ...` commands remain available
for same-repository draft PRs with their existing authorization rules.

Validate job admission with `node --test .github/scripts/heavy-ci-policy.test.cjs`.
These tests verify triggering decisions, not runner availability or performance.

## Narthex branch

GitHub schedules run only on the default branch. They do not test `narthex`
automatically. To validate this branch, manually dispatch Rust CI, Integration
CI (selecting a fuzz profile), or Nightly CI with `narthex` as the ref.

Land these scope rules on `narthex` before updating dependent PR branches:
the classifier checks out the PR base revision, not the PR head.
