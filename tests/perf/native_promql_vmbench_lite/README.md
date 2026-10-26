# Native PromQL VMbench lite

This standalone Mito benchmark exercises non-append ingestion (one region per metric) and multi-scan partitions. Both request lanes run against the same binary with the hint disabled and enabled. The correctness plan and frozen dataset are shared across lanes; freeze data once per run and reuse those exact bytes. Before/after optimization runs must reuse the published frozen dataset, not independently regenerate it from a CLI seed.

The canonical smoke interval is 2025-03-21 09:50–10:20 UTC at 30-second steps. The initial immutable CI smoke runs 12 queries across both lanes and checks correctness; it does not qualify an improvement and is not a baseline. After the CI smoke passes, collect the baseline by updating the descriptor to `phase = "baseline"`, `serial_warmup = 10`, `serial_tries = 50`, `duration_seconds = 60`, and `warmup_seconds = 10`. #9556 has unknown workload and no historic number for comparison. A real baseline must precede a performance conclusion; local performance conclusions are forbidden (CI results only).

Use the existing query-regression workflow for both candidate and base builds, reporting, and cleanup. The pinned O11y benchmark adapter is a fixed standalone Python path; this is not a Rust scenario DSL case. Its output logs include full results, plans, raw JSONL, config, dataset manifest, health, process CPU, and RSS. The fallback forecast and observed physical path are both reported. HTTP 200 alone does not establish Native execution: qualification requires full-result equality and actual scan/Fused/Finalize evidence. Four physical scan partitions do not imply that all four process data in this small fixture.

Dispatch directly:

```sh
gh workflow run query-regression.yml --repo GreptimeTeam/greptimedb --ref main \
  -f base_ref=<full-narthex-SHA> -f candidate_ref=<full-candidate-SHA> \
  -f case=tests/perf/native_promql_vmbench_lite/benchmark.toml \
  -f cargo_profile=release -f runner=aliyun-ecs \
  -f allow_large_fixture=true -f http_timeout=300
```
