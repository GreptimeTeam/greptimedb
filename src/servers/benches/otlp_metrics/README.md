# OTLP metrics benchmarks

Run from the workspace root:

```sh
cargo bench -p servers --bench otlp_metrics
cargo bench -p servers --bench otlp_metrics_preflight
```

The conversion benchmark measures `to_grpc_insert_requests`, including point
preparation, encoding, survivor merging, and optional resource descriptors. The
preflight benchmark measures destination validation against existing tables in
an in-memory catalog, with a fresh per-request schema cache. Request cloning,
fixture construction, and catalog setup are outside the timed section. Neither
benchmark measures network requests, table creation, storage writes, or the full
ingestion endpoint. Both use the server's jemalloc allocator on non-Windows hosts.

Cases cover one point, 1,024 points with eight tags, 64 tags, classic histogram
fan-out, cumulative and delta exponential histograms, and mixed valid/invalid
points. `mixed_1024` rejects every tenth point during conversion;
`required_tag_1024` converts all points but rejects every tenth during preflight
because an ordinary table requires a missing tag. Other preflight cases use
logical tables with a shared physical schema. Each case checks accepted or
rejected counts before measuring. Conversion runs with descriptors on and off;
preflight does not validate the separate descriptor write.

To compare with `baa5630119`, copy `otlp_metrics.rs` and `otlp_metrics/fixtures.rs`
into an isolated checkout of that revision and register the `otlp_metrics` bench
in `src/servers/Cargo.toml` with `harness = false`. Use the same toolchain, build
profile, and machine for both revisions. Run serially, with other CPU-heavy work
stopped, and store both runs in the same absolute `CRITERION_HOME`:

```sh
# In the base checkout:
cargo bench -p servers --bench otlp_metrics -- otlp_conversion/common --save-baseline main
# In the feature checkout:
cargo bench -p servers --bench otlp_metrics -- otlp_conversion/common --baseline main
```

Delta conversion and preflight are measured only on the feature branch because
the base does not support them. A before/after conversion ratio measures the
whole branch's conversion changes, not the overhead of preflight. These are local
microbenchmarks; use an ingestion workload to measure end-to-end throughput.
