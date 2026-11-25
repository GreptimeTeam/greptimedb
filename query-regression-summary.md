## ✅ Query regression report: `default case set`

- **Status:** `ok`
- **Case path:** `/home/discord9/greptimedb/.e-agent/worktrees/dist-join-materialize/tests/perf/query_cases/sql_join_dist_broadcast/case.toml`
- **Query mode:** `endpoint`
- **Artifacts:** query-regression-work logs, fixture metadata, and JSON report are uploaded with this run.

### Targets

| Target | Status | Validation errors | Region | Datanode data home |
| --- | --- | ---: | --- | --- |
| base | ✅ measured | 0 | N/A | `N/A` |
| candidate | ✅ measured | 0 | N/A | `N/A` |

### Query comparison

| Query | Base median ms | Base p95 ms | Candidate median ms | Candidate p95 ms | Regression | Threshold |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| join_dist_broadcast_aggregate_top | 76.42 | 83.32 | 49.89 | 53.66 | -34.7% | max_candidate_latency_regression_pct: passed |
| join_dist_broadcast_volatile | 71.25 | 74.16 | 40.59 | 51.19 | -43.0% | max_candidate_latency_regression_pct: passed |
