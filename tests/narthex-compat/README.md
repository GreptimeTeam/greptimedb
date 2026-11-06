# Narthex native CI

Three jobs check the native PromQL route independently. They live in
`.github/workflows/narthex-native-ci.yml`; the scripts they run are under `.github/scripts/` and
`tests/narthex-compat/`.

| Job | What it proves | Where |
| --- | --- | --- |
| `promql-sqlness` | The whole standalone environment still answers, with the native route switched on, against its own `.result` | `narthex-native-route.py` |
| `prometheus-compat` | GreptimeDB's answers match the pinned Prometheus engine, case by case | `tests/narthex-compat/`, `narthex-prometheus-compat.py` |
| `vm-performance` | Candidate, the `narthex` baseline and VictoriaMetrics, on one machine | `.github/workflows/vmbench-long-range.yml` (reused) |

## Scope: standalone

The sqlness job runs the standalone environment only. Distributed is out of scope for
now; see the note below.

## Where the native route can run

The native route needs `in_process_region_transport`, which only the standalone server installs
(`src/cmd/src/standalone.rs`). A distributed server answers every PromQL statement on the existing
route, so the job runs `--env-filter '^standalone$'`.

`distributed` cases share their `.result` with `standalone` through the `distributed/common` symlink.
They are not part of this job: the switch cannot change their plan, so a green distributed run would
say nothing about the native route.

## What the job runs

`sqlness-runner bare -c tests/cases --enable-native-promql --env-filter '^standalone$'`, against the
checked-in expectations, with no copy of the case tree. That is sqlness' own workflow: it compares
each case with its `.result` and writes the output it produced into that same file. A case that
disagrees fails the run, and the job records `git diff -- tests/cases` as an artifact, so a reviewer
reads the change against the commit.

Accepting a new expectation is therefore the ordinary loop: run, read the diff, commit it. The next
run passes because the file now holds the reviewed output - which is the point, not an accident.

Covering the whole environment is deliberate. The switch is a server-wide setting, so any case could
see it; a selector that picked only the files mentioning PromQL would also have to stay correct as
new directories appear.

## Route evidence

A passing `.result` does not say which route answered. `narthex-native-route.py` reads the
`.result` files a run wrote and classifies each statement:

- `native` when the statement's plan holds `FusedSeries`;
- `legacy` when it holds a legacy evaluator (`PromInstantManipulateExec`, ...) and no fused node.

Only `TQL EXPLAIN` / `TQL ANALYZE` statements report their route; an ordinary PromQL statement
reports nothing, and the tool says so instead of guessing. That is why the route check is scoped to
the cases that ask for a plan, rather than claiming every case ran natively.

## The two switches

Setting one without the other silently runs the existing route, so the runner takes a single flag:

- `--enable-native-promql` renders `[query] experimental_native_promql = true` into the generated
  config **and** sends `query.experimental_native_promql=true` as a per-request hint.

## Prometheus conformance

`tests/narthex-compat/oracle` evaluates a case file with the real Prometheus engine, pinned to
`v0.315.1` (the Go module version of Prometheus 3.15.1). The checked-in goldens under
`src/promql/src/**/prometheus_cases.txt` were generated from Prometheus 3.15.0; this oracle tracks
the latest stable patch of the same line.

The comparison covers result type, label sets including `__name__`, timestamps, values, missing
points, extra series, NaN and the infinities, and queries both engines must refuse. Floats use a
relative tolerance of `1e-9`; nothing else is normalized.

`tests/narthex-compat/KNOWN-DIFFERENCES.md` lists the differences the harness found. A listed
difference is still printed and counted separately as `known-difference`; it is never silently
dropped, and the entry may not cover values, timestamps, missing points or extra series.

## Performance

`vm-performance` calls `vmbench-long-range.yml`, which runs GreptimeDB and VictoriaMetrics on one
provisioned machine and supports a second GreptimeDB target for comparison. The call is gated to the
nightly schedule and a manual dispatch: the benchmark provisions an Aliyun ECS or AWS EC2 runner, and
a pull request from a fork never receives the credentials that needs.

What it measures today is not this build. The job passes no image tags, so the candidate is the
published `greptime/greptimedb:latest` and the comparison target is `greptime/greptimedb:v1.2.1`,
both Docker Hub defaults. Measuring the ref under test needs an image built and pushed from it, which
this workflow does not do yet.

Not covered, and therefore not claimed: cold-process first-query timing, per-query regression
thresholds derived from repeated runs, and the stall where a query hangs behind a metadata load that
holds a stripe (#10114). Those need the runner's own measurements before they can gate anything.
