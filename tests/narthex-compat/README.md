# Narthex native CI

Three jobs check the native PromQL route independently. They live in
`.github/workflows/narthex-native-ci.yml`; the scripts they run are under `.github/scripts/` and
`tests/narthex-compat/`.

| Job | What it proves | Where |
| --- | --- | --- |
| `promql-sqlness` | Every PromQL sqlness case still answers, with the native route switched on, against its own `.result` | `narthex-promql-cases.py`, `narthex-native-route.py` |
| `prometheus-compat` | GreptimeDB's answers match the pinned Prometheus engine, case by case | `tests/narthex-compat/`, `narthex-prometheus-compat.py` |
| `vm-performance` | Candidate, the `narthex` baseline and VictoriaMetrics, on one machine | `.github/workflows/vmbench-long-range.yml` (reused) |

## Scope: standalone

The sqlness job runs the standalone environment only. Distributed is out of scope for
now; see the note below.

## Where the native route can run

The native route needs `in_process_region_transport`, which only the standalone server installs
(`src/cmd/src/standalone.rs`). A distributed server answers every PromQL statement on the existing
route. Consequences, both reflected in the selector below:

- `promql-sqlness` runs the native snapshot comparison for **standalone** cases.
- `distributed` cases keep a `.result` equal to `.result`. That is a compatibility check -
  "the switch does not break the distributed answer" - and is **not** evidence that a distributed
  statement ran natively.

## Selecting the cases

`narthex-promql-cases.py` reads the statements in every `.sql` under `tests/cases` and selects the
ones holding a PromQL statement. It follows symlinks and de-duplicates, so the shared
`distributed/common` tree is not run twice, and it ignores comments, so a fixture that mentions
PromQL while explaining it is plain SQL is not selected.

Selection is by content, not by directory name: PromQL cases live under `common/promql`, but also
under `common/tql`, `flow-tql`, `optimizer` and `tql-explain-analyze`, and a new directory would
otherwise be missed.

## Why the run is staged

`sqlness` filters cases by `environment:file_stem`, which cannot distinguish
`common/promql/offset` from `common/subquery/offset` in the same environment. It also rewrites the
expectation file it just read before reporting the diff. Both are handled by staging:

1. `--command stage` builds a temporary tree holding only the selected cases. The `.sql` files are
   symlinks to the repository, so there is still one copy of every statement.
2. The expectation is a **copy** of the committed `.result`, so a run that writes a wrong
   answer cannot make the next run agree with it - the copy is discarded every time.
3. `--command collect` prints the staged expectations that a run rewrote as a diff against the
   committed ones. Nothing is copied back automatically; a changed expectation is reviewed.

A case with no committed `.result` fails the job **before** any server starts
(`--command check`), so a new PromQL case cannot pass by quietly reading the ordinary expectation.

## Route evidence

A passing `.result` does not say which route answered. `narthex-native-route.py` reads the
run's own output and classifies each statement:

- `native` when the statement's plan holds `FusedSeries`;
- `legacy` when it holds a legacy evaluator (`PromInstantManipulateExec`, ...) and no fused node.

Only `TQL EXPLAIN` / `TQL ANALYZE` statements report their route; an ordinary PromQL statement
reports nothing, and the tool says so instead of guessing. That is why the route check is scoped to
the cases that ask for a plan, rather than claiming every case ran natively.

## The two switches

Setting one without the other silently runs the existing route, so the runner takes a single flag:

- `--enable-native-promql` renders `[query] experimental_native_promql = true` into the generated
  config **and** sends `query.experimental_native_promql=true` as a per-request hint.

The result file is chosen separately by `--result-extension result`. Choosing the file does
not enable the route.

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

`vm-performance` reuses `vmbench-long-range.yml`, which already runs GreptimeDB and VictoriaMetrics
on one prepared machine and supports a second GreptimeDB target for comparison. This workflow passes
the `narthex` baseline as that second target and the PR's build as the candidate.

Not yet covered, and therefore not claimed: cold-process first-query timing, per-query regression
thresholds derived from repeated runs, and the columnar metadata cache deadlock that used to make a
cold Q09 time out (#10114). Those need the runner's own measurements before they can gate anything.
