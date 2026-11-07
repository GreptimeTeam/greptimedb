# Fuzz Test for GreptimeDB

## Setup
1. Install the [fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html) cli first.
```bash
cargo install cargo-fuzz
```

Note: `cargo-fuzz` instruments targets with `-Zsanitizer=fuzzer`, so fuzz
targets must be built with a **nightly** toolchain (the workspace default
toolchain does not need to be nightly — `make fuzz` / `make fuzz-ls`
invoke `cargo +nightly` for you; CI pins its own nightly in
`FUZZ_RUST_TOOLCHAIN`).

2. Start GreptimeDB
3. Copy the `.env.example`, which is at project root, to `.env` and change the values on need.

### For stable fuzz tests
Set the GreptimeDB MySQL address.
```
GT_MYSQL_ADDR = localhost:4002
```

### For unstable fuzz tests
Set the binary path of the GreptimeDB:
```
GT_FUZZ_BINARY_PATH = /path/to/
```

Change the instance root directory(the default value: `/tmp/unstable_greptime/`)
```
GT_FUZZ_INSTANCE_ROOT_DIR = /path/to/
```
## Run
1. List all fuzz targets
```bash
cargo fuzz list --fuzz-dir tests-fuzz
```

2. Run a fuzz target.
```bash
cargo fuzz run fuzz_create_table --fuzz-dir tests-fuzz -D -s none
```

## CI Corpus Persistence

The shared `.github/actions/fuzz-test` action restores inputs before running
the selected targets and saves the updated corpus afterward, including when a
target fails. Cache failures are best-effort and do not change the fuzz result.
Crash files, diagnostic artifacts, and fail-fast behavior are unchanged.

Each target reads and writes its own `tests-fuzz/corpus/<target>` directory.
Both prebuilt executables and the `cargo fuzz` fallback receive that directory
explicitly. For local runs of `.github/scripts/run-fuzz-targets.sh`, set
`FUZZ_CORPUS_ROOT` to use a different corpus root.

The selected target directories are cached together to preserve the existing
job grouping. The stable key prefix identifies the sorted, deduplicated target
set, not a source hash, so code changes do not invalidate existing inputs.
Each job saves a new immutable entry, distinguished by run ID, attempt, job,
and matrix index. Restore uses the latest accessible entry matching the prefix.
Changing the target set starts a different cache; corpora are not shared between
different target sets. Parallel jobs may start from the same entry, but their
new inputs are not merged automatically.

The restore step logs the matched key (including prefix matches) or a cache
miss. GitHub's [cache access restrictions](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching#restrictions-for-accessing-a-cache)
still apply: PR caches are scoped to that PR, while a default-branch cache can
seed other PRs. The existing scheduled workflow does not run fuzz jobs; a
maintainer can populate the default-branch corpus through the existing manual
fuzz workflow. This change does not add scheduling, target selection, or corpus
pruning. Rolling entries remain subject to GitHub's cache limits and eviction.

### Validation

The orchestration tests use mock fuzz executables and do not build Rust or
start GreptimeDB. They cover both runner paths, restored inputs, separate
target directories, retained inputs after failure, cache identity, and invalid
target names. Run them on Linux with Bash, jq, and GNU coreutils:

```bash
bash .github/scripts/run-fuzz-targets-test.sh
```

These tests do not exercise the hosted GitHub cache service. To verify that
service, inspect two fuzz workflow runs on the same PR: the first should save
an entry, and the second should report its restored key and load a nonempty
corpus. Also verify that cache save still runs after a target failure, without
turning that failure into a successful fuzz result.

To compare useful coverage, use the same fuzz executable, GreptimeDB revision,
service configuration, and time budget for empty and restored corpora. Both
runner paths enable `-print_final_stats=1`; the target's `fuzz.log` retains the
libFuzzer coverage and final statistics. Seed the restored corpus with a
separate warm-up run (or a downloaded corpus), then compare it with a fresh,
empty directory. Keep warm-up time separate from the comparison and repeat
trials to account for randomness. Report initial corpus size, final covered
edges (`cov:`), executions, and elapsed time, rather than inferring a coverage
improvement merely from a cache hit.

For example, with a prebuilt `fuzz_create_table` binary and a running standalone
service, the following creates separate warm-up, empty, and seeded corpora. Set
`FUZZ_BIN_DIR` to the absolute binary directory and `GT_MYSQL_ADDR` to the test
service address first. All runs use the same executable and 120-second budget;
the warm-up is not part of the measured comparison. This creates temporary
inputs, logs, and crash files, and executes DDL against the test service.

```bash
set -euo pipefail
: "${FUZZ_BIN_DIR:?Set the prebuilt fuzz binary directory}"
: "${GT_MYSQL_ADDR:?Set the test service address}"
comparison_dir="$(mktemp -d -t fuzz-corpus-comparison.XXXXXX)"
for trial in seed cold warm; do
  mkdir -p "${comparison_dir}/${trial}" "${comparison_dir}/artifacts/${trial}"
  if [[ "${trial}" == warm ]]; then
    cp -a "${comparison_dir}/seed/." "${comparison_dir}/warm/"
  fi
  "${FUZZ_BIN_DIR}/fuzz_create_table" "${comparison_dir}/${trial}" \
    -max_total_time=120 -print_final_stats=1 \
    "-artifact_prefix=${comparison_dir}/artifacts/${trial}/" \
    2>&1 | tee "${comparison_dir}/${trial}.log"
done
printf 'Comparison logs and corpora: %s\n' "${comparison_dir}"
```

## CI Target Selection

PR runs select targets inside the existing job groups using the PR base's
`.github/scripts/select-fuzz-targets.py` and `fuzz-target-rules.toml`. Direct
target edits map through `tests-fuzz/Cargo.toml`; the rules also cover the
alter and repartition generators and MySQL translators. Metric-engine source
changes select all metric targets (logical tables, repartition, migration,
failover, and WAL pruning). Matching rules are combined. Other shared
application code, unmapped paths, changed target declarations,
an incomplete or stale API diff, and selection errors run the complete set.
Renames consider both old and new paths. The first target in each existing
group always runs as a smoke test, including groups unaffected by the diff.

This is target selection, not job admission. Job names, service modes, binary
builds, time budgets, and diagnostics stay the same. The existing full binary
archive is still built. Selection can reduce target executions, but does not
claim to reduce build or cluster startup time. Suite-level admission remains
separate from this selector. The scope classifier admits jobs first; target
selection only narrows the executions inside admitted groups. A selector result
cannot start a job excluded by scope, event policy, or a failed script preflight.

The complete group still identifies the corpus cache, even when only some
targets execute. Unselected restored inputs are retained. The optional
`affected-targets` action input carries the selection decision; empty or
invalid decisions run the complete group. The first PR introducing the policy
also runs everything because the script does not yet exist on its base.

Manual fuzz runs use the complete requested profile, and scheduled fuzz runs
use all targets. The upstream event policy runs PR fuzzing on opening a ready
PR or marking it ready for review, skips ordinary pushes/reopening and merge
groups, and keeps core integration checks on updates. On PRs, the `ci:full`
label or `CI_SCOPE_FORCE_ALL` repository variable disables target narrowing;
these overrides do not change job admission. See [Heavy CI scheduling](../.github/heavy-ci.md)
for the event policy. Logs show the decision, matched paths, and each group's
final target list and smoke target. The shared script-test action runs the
selector tests before expensive jobs in Checks, Rust CI, and Integration CI.

Test the selector with Python 3.11 or newer, without building Rust or starting
services:

```bash
python3 -m unittest discover -s .github/scripts/tests -p 'test_select_fuzz_targets.py'
bash .github/scripts/run-fuzz-targets-test.sh
```

Before narrowing another shared helper, trace all target consumers and update
the mapping and tests together. New or unknown paths deliberately run the full
set until they have an explicit safe mapping.

## Crash Reproduction
If you want to reproduce a crash, you first need to obtain the Base64 encoded code, which usually appears at the end of a crash report, and store it in a file.

Alternatively, if you already have the crash file, you can skip this step.

```bash
echo "Base64" > .crash
```
Print the `std::fmt::Debug` output for an input.

```bash
cargo fuzz fmt fuzz_target .crash --fuzz-dir tests-fuzz -D -s none
```
Rerun the fuzz test with the input.
You can override fuzz input with environment variables. For example, to override fuzz input like:

```
FuzzInput {
    seed: 6666,
    actions: 175
}
```

you can run with `GT_FUZZ_OVERRIDE_SEED=6666` and `GT_FUZZ_OVERRIDE_ACTIONS=175`:

```bash
GT_FUZZ_OVERRIDE_SEED=6666 GT_FUZZ_OVERRIDE_ACTIONS=175 cargo fuzz run fuzz_target .crash --fuzz-dir tests-fuzz -D -s none
```

For more details, visit [cargo fuzz](https://rust-fuzz.github.io/book/cargo-fuzz/tutorial.html) or run the command `cargo fuzz --help`.

## Repartition Metric Dump Artifacts

For `fuzz_repartition_metric_table`, dump artifacts are written under one run directory.

- Table data snapshots: `<logical_table>.table-data.csv`
- SQL traces per logical table: `<logical_table>.trace.sql`
- Seed metadata: `seed.meta`

SQL trace behavior:

- Insert SQL is appended after successful execution with comment fields including
  `started_at_ms` and `elapsed_ms`.
- Repartition events are broadcast to all logical table trace files with comment fields including
  `action_idx`, `started_at_ms`, `elapsed_ms`, and SQL text.

Run directory lifecycle:

- On success, the run directory is cleaned up.
- On failure, the run directory is retained for CI/local diffing.
