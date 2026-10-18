# Fused-series benchmark artifacts

These files are the measurement harness and the raw records behind
`FusedSeriesExec`. They are kept on this branch, **not** in the pull requests
that add the code they measure: a PR should carry the change and its regression
tests, and the harness would double its review surface without testing anything
the unit tests do not already cover.

Contents:

| Path | What it is |
|---|---|
| `scripts/fused_series_bench.py` | Driver: runs one arm of a comparison and writes a JSON record. |
| `scripts/fused_series_bench_crossed.sh` | Crossed-window matrix (window width x grid) driver. |
| `scripts/fused_series_bench_pair.sh` | A/B driver: ordinary plan vs fused node on the same fixture. |
| `scripts/fused_series_bench_summarize.py` | Summarizes one or more JSON records into ratios. |
| `scripts/fused_series_bench_pair_summarize.py` | Summarizes paired A/B records. |
| `docs/bench-records/<timestamp>Z/` | Raw JSON records plus the `RESULTS.md` each run produced. |
| `docs/fused-series-report.md` | What was built, what was measured, and what the measurements do **not** show. |
| `src/promql/benches/bench_range_fn.rs` | Criterion bench for the range evaluator kernels. Compiles against the current `narthex` API. |

To reproduce a pair, build the two arms, then run
`scripts/fused_series_bench_pair.sh` and summarize with
`scripts/fused_series_bench_pair_summarize.py`. The report explains which
scenarios (`cC`, `cG`, `cS`, `cW`, `ccontrol`) mean what.

The report is deliberately pessimistic about the results. Read its "what the
measurements do not show" section before quoting any number from it.
