# Integration CI admission gate

`integration.yml` runs its expensive jobs — fuzz, export/import E2E, the sqlness
KvBackend legs — on every non-draft pull request. Most pull requests cannot
affect any of them. This directory decides which of those jobs start, using only
the diff.

The gate can only *skip* work, never add it, and it fails open: when the verdict
is missing or uncertain, every gated job runs.

| File | Role |
|---|---|
| `ci_scope.py` | the classifier: reads the diff, writes `$GITHUB_OUTPUT` |
| `scope-rules.toml` | the policy: which paths select which suite, as data |
| `ci_scope_rules.py` | loads and validates the policy; `--check-paths` |
| `tests/` | unit tests, and `fixtures/scope-cases.toml` golden cases |

Two words appear everywhere below. A **suite** is one matrix leg of a gated job,
named `<gate>/<leg>`. A **gate** is one `$GITHUB_OUTPUT` key that switches a
whole job.

## What is gated

Only `integration.yml`, and only these jobs:

| Gated job | Runs |
|---|---|
| `prepare-fuzz-targets` | the fuzz targets only |
| `fuzztest` | standalone fuzz, 2 legs |
| `unstable-fuzztest` | unstable standalone create-table fuzz, 1 leg |
| `distributed-fuzztest` | distributed fuzz with remote WAL, 4 legs |
| `distributed-fuzztest-with-chaos` | Chaos Mesh, 3 legs |
| `build-greptime-ci` | the profile-CI binary only |
| `export-import-v2-e2e` | CLI `COPY DATABASE` export/import against S3 |
| `sqlness-kvbackend` | sqlness with PostgreSQL and MySQL as the metadata store |

The legs themselves are the `matrix` entries in `integration.yml`; the rules
file repeats their ids so a verdict can name them.

Everything else always runs: `rust.yml`, `checks.yml`, `docs.yml`, the debug
`build`, the required `sqlness` job (Basic, Remote WAL, Flat format), `compat`
and `compat-updater-check`. None of the gated jobs is in the required-check
matrix in `docs.yml`, so a skipped job can never block a merge.

Two choices follow from that rule. The two KvBackend legs sit in their own
`sqlness-kvbackend` job because a job-level `if:` cannot read `matrix`, so
gating them inside `sqlness` would skip required checks and make the pull
request unmergeable. The multi-language SDK job stays ungated: it takes about
three minutes, less than the cost of getting its verdict wrong.

## How a verdict is made

First match wins:

1. **Forced** — `merge_group`, `schedule`, `workflow_dispatch`, the `ci:full`
   label or `CI_SCOPE_FORCE_ALL`. Everything runs.
2. **Too large to judge** — more than 300 changed files. Everything runs.
3. **Always-run file changed** — `Cargo.toml`, `Cargo.lock`, `tests-fuzz/**`,
   this directory, or anything else that changes what the suites build or how
   they start. Everything runs.
4. **Uncovered change** — a path under `src/` that no suite claims. This is the
   net for a crate added, split or renamed after the rules were written.
   Everything runs.
5. **Path rules** — the suites whose globs match the changed files, and only
   those.

The code is the source of truth for the details; its module docstring is worth
reading before changing it.

## Why a check was skipped

The run page's summary pane answers this without opening a log: one row per
suite, with its verdict, the rule that decided it, and up to three of the
changed files that matched.

The step log of the `CI Scope` job has the same information in text form. It
starts with `CI scope: mode=… full=…` (`grep '^CI scope'` finds it), then one
line per suite — `run` or `skip`, source, reason — then `gates:` with the exact
values the jobs read.

A degraded decision is never silent. A failed label or file lookup emits
`::warning::` and widens the gate to every suite.

## Forcing a full run

Add the `ci:full` label to the pull request and click "Re-run all jobs". The
label is read through the API at run time, not from the event payload, so one
added after the first run still counts. `/ci` dispatch
(`.github/scripts/ci-slash.py`), the `CI_SCOPE_FORCE_ALL` repository variable
and every merge-queue run also run everything; the classifier returns a forced
plan there, so a wrong skip costs one queue failure, not a broken `main`.

`CI_SCOPE_FORCE_ALL` is the switch to flip while debugging the rules themselves.

## Changing the rules

`scope-rules.toml` is data, not code: `ci_scope_rules.py` loads it with the
standard library's `tomllib`. Two things are validated:

- the shape — an unknown or cyclic `@group` reference, a `gate` that disagrees
  with the suite id, a gate missing from `[gates]`, or an exemption without a
  `reason` all raise;
- that every glob still points at something that exists, so a renamed or
  deleted crate fails the check instead of silently matching nothing.

Run both with `make check-ci-scope`, or the path half alone with
`python3 .github/ci/ci_scope_rules.py --check-paths`. The same command runs in
the `CI Scope Rules` job of `checks.yml`, on `ubuntu-slim` — the image the
classifier itself uses, so a path that exists there also exists when the verdict
is computed.

A malformed file fails the classify step, and because every step of that job is
`continue-on-error`, every gated job runs instead.

`[[matching.exempt]]` lists paths outside the gate's remit: they select no suite
and never widen the gate. Each one carries a `reason`, and the loader rejects an
empty one — what CI never runs is the part a reader must be able to justify.

`tests/fixtures/scope-cases.toml` is test data, not configuration: a diff shape
and the suites it must start. `ScopeCaseTests` replays each case through the
classifier, so a rule edit that changes behaviour fails with the case name
instead of silently rerouting CI. The cases carry no pull request numbers;
`git log` has those.

## Adding a suite

1. Add a `[suites."<gate>/<leg>"]` table to `scope-rules.toml` with `gate`,
   `paths` (globs or `@group` references) and a one-line `description`. The id
   is `<gate>/<matrix leg id>`, and `gate` is that id with `-` replaced by `_`;
   the loader and `test_suite_metadata` both enforce it.
2. Add the matching matrix leg to the job in `integration.yml`.
3. Gate the job's `if:` with the matching output, and list `scope` in its
   `needs`. A new gate also needs a key in `derive_outputs` and in the `scope`
   job's `outputs`; `test_every_gate_routes_to_an_output` fails when that
   routing is missing.
4. Add a case to `tests/fixtures/scope-cases.toml` and run
   `python3 .github/ci/tests/test_ci_scope.py`. `CrateCoverageTests` separately
   fails when a workspace crate matches no suite and is not excused in
   `UNMAPPED_CRATES`: the runtime net would run everything, but the rule table
   is what should be updated.

## Ground rules for the workflow

These six rules keep the gate failing open. Break one and either CI runs jobs it
should have skipped, or a classifier failure takes CI down with it.

- **Read the output as `!= 'false'`.** An absent output means "run"; writing
  `== 'true'` inverts the failure direction.
- **Start every gate with `!cancelled()`.** Without a status function the
  condition carries an implicit `success()` over `needs`, so a `scope` job that
  times out would skip the gates. Other dependencies are therefore required
  explicitly with `needs.<job>.result == 'success'`.
- **Every fallible step of `scope` is `continue-on-error: true`.** GitHub skips
  the dependents of a *failed* job, so a classifier crash must leave the job
  green with no outputs rather than take CI down with it.
- **Rules come from the base revision.** The `scope` job checks out
  `pull_request.base.sha` (or `merge_group.base_sha`), so a pull request cannot
  weaken its own gate. On the first pull request that introduces this directory
  the base has no `ci_scope.py`: the step fails, no outputs are written, and
  every gate runs.
- **One gate per job, never per leg.** GitHub does not expose `matrix` to
  `jobs.<job_id>.if`. Per-suite verdicts stay visible in the log.
- **Pass the run's `head`/`base` SHAs and let the classifier compare them with
  the pull request's live revisions.** A re-run keeps the original SHAs while
  GitHub serves the *current* file list; without the comparison the new diff
  would be judged against the old tree. With it, a mismatch runs everything.
