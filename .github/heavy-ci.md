# Heavy CI scheduling

Rust CI keeps the Windows compilation check on PRs and merge groups. Windows
nextest and Windows SQLness run in the existing Nightly CI workflow, which also
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
