# OTLP metrics ingestion benchmarks

Run from the repository root. The benchmark uses deterministic requests and the
same OTLP protobuf type and converter as HTTP ingestion. No ingestion code needs
to change to record the baseline. It uses the production binary's jemalloc
allocator on non-Windows platforms.

## CPU baseline and comparison

```sh
cargo bench -p servers --bench otlp_metrics -- otlp_metrics --save-baseline before
# After changing ingestion code, on the same machine and toolchain:
cargo bench -p servers --bench otlp_metrics -- otlp_metrics --baseline before
```

Criterion stores samples, estimates and comparison reports under
`target/criterion` (or `$CRITERION_HOME` if set). Preserve this directory between
runs. Use a new baseline name for a new experiment; `--save-baseline` overwrites
an existing name. To check all fixtures without collecting measurements:

```sh
cargo bench -p servers --bench otlp_metrics -- --test
```

Each workload has three measurements:

| Measurement | Included work |
| --- | --- |
| `decode` | Protobuf bytes to a fresh request; dropping the request |
| `convert` | Request to rows, semantic metadata and outcome; dropping the result |
| `decode_to_rows` | Both stages together, including dropping the result |

For `convert`, decoding and constructing the context happen outside the timer,
once per iteration. All measurements release their output within the timer.
Input serialization is outside every timer; no growing `merge()` state or
request cloning is included in conversion measurements. A fresh context is used
each time. The small per-iteration timer overhead is shared by before/after runs.

`decode` remains the stock-protobuf control if a specialized decoder is added.
At that point, wire `decode_to_rows` to the new production decoding entry point;
retain the fixture bytes, workload names, throughput units and timing boundary.
The HTTP benchmark automatically exercises whichever decoder the server uses.

| Workload | Resources | Metrics/resource | Points/metric | Extra point attributes | Input points | Output rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `small` | 1 | 4 | 1 | 2 | 4 | 4 |
| `shared` | 1 | 16 | 64 | 8 | 1,024 | 1,024 |
| `resources` | 32 | 4 | 8 | 8 | 1,024 | 1,024 |
| `wide` | 1 | 16 | 64 | 32 | 1,024 | 1,024 |
| `delta_sum` | 1 | 16 | 64 | 8 | 1,024 | 1,024 |
| `histogram_10` | 1 | 4 | 64 | 8 | 256 | 3,328 |
| `histogram_50` | 1 | 4 | 64 | 8 | 256 | 13,568 |
| `summary` | 1 | 4 | 64 | 8 | 256 | 1,280 |

All cases include promoted resource attributes, promoted scope metadata, a
per-point series identifier, and mixed string/integer point attributes. Larger
cases introduce an optional column after initial rows to exercise schema growth
and null filling. Repeated metric names across resources share output tables.
Histogram cases use classic cumulative histograms; exponential histograms and
resource descriptors are deliberately excluded from this experiment.

The fixture checks run before timing and verify acceptance, emitted row/table
counts and row/schema alignment. The benchmark prints bytes, points, rows and
tables per request. Criterion throughput is **input data points/sec**; multiply
by the listed output/input ratio for emitted rows/sec. These timings do not
measure allocations, network, table lookup, DDL or storage.

## HTTP write baseline and comparison

Use a dedicated standalone instance and a fresh disposable database for each
run. The benchmark only writes when both `OTLP_BENCH_URL` (the complete metrics
endpoint) and `OTLP_BENCH_DB` are supplied. It does not start/stop servers, create
databases or delete data. For example, after starting a local server on port
14000:

```sh
curl --fail-with-body http://127.0.0.1:14000/v1/sql \
  --data-urlencode 'sql=CREATE DATABASE otlp_bench_before'
OTLP_BENCH_URL=http://127.0.0.1:14000/v1/otlp/v1/metrics \
OTLP_BENCH_DB=otlp_bench_before \
  cargo bench -p servers --bench otlp_metrics -- otlp_http --save-baseline http-before
```

For the candidate, restart with a fresh data directory, create
`otlp_bench_after`, and run the same command with `OTLP_BENCH_DB=otlp_bench_after`
and `--baseline http-before`. Keep the Criterion output directory intact.

This runs scalar gauge workloads at concurrency 1 and 32. One iteration is a
round of concurrent requests; it waits for every response before the next round.
Criterion reports **round duration**, not individual-request p95/p99 latency.
Throughput includes all input points in a round. Request generation/serialization
is excluded, connections are reused, and table creation is warmed before timing.
Timestamps advance for every request, including warm-up, to avoid benchmarking
repeated overwrites. HTTP errors and OTLP partial failures fail the run. After
each workload, an untimed SQL query checks the total persisted row count.

Run both binaries with identical settings and synchronous acknowledgements
(`PENDING_ROWS_BATCH_SYNC=true`, the default). Record `with_metric_engine`,
`pending_rows_flush_interval`, row/flush limits, hardware, toolchain, commit,
storage directory setup and workload filter alongside results. Test batching
disabled first, then repeat with the same nonzero interval on both binaries;
record those as separate baselines. Default batching is disabled. Comparing
enqueue acknowledgements with completed writes would not be equivalent.

Run the load generator on a separate machine for server capacity measurements.
A local client and server share CPU and are useful for smoke checks and local
comparisons, but not an isolated server throughput result. This harness measures
warm-schema writes; cold DDL, fixed-rate tail latency, compression and allocation
profiling are separate experiments.
