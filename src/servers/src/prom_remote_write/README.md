# Prometheus Remote Write

This module decodes Prometheus remote write requests and converts them into
Greptime row insert requests.

## Remote Write V2

Remote write v2 enters through `remote_write_v2` in
`src/servers/src/http/prom_store.rs`.

```mermaid
flowchart TD
    A["HTTP /v1/prometheus/write"] --> B["remote_write_v2"]
    B --> C["decode_remote_write_v2"]
    C --> D["borrow symbols and time-series bytes"]
    D --> E["decode leaf messages and build rows"]

    E --> F["samples ContextReq"]
    E --> G["histograms ContextReq"]

    F --> H["write_prometheus_rows_with_progress"]
    H --> I["metric engine / pending batcher when enabled"]

    G --> J["histogram ContextOpt"]
    J --> K["write_prometheus_rows_with_progress"]
    K --> L["same metric-engine flag as samples, no batcher"]
    L --> M["table: <metric>"]

    M --> N["field: configured native-histogram Struct"]
    N --> O["struct children: counts, spans, buckets, sum, schema"]
    I --> P["written headers and counters"]
    L --> P
```

The conversion step splits one v2 request into two `ContextReq`s:

The `decode` codec metric covers decompression and the borrowed request
envelope. The `convert` metric covers per-series scans, leaf prost decoding,
validation, and row construction.

- samples keep the existing sample table name and can use the metric-engine
  physical table path and pending rows batcher;
- native histograms keep the existing metric table name.

Native histogram rows follow the same metric-engine switch as samples. They do
not use the pending rows batcher yet because the batcher assumes the classic
timestamp + Float64 value + string tags shape.

Each histogram row stores one Struct field named
`<default_column_prefix>_native_histogram`. The default name is
`greptime_native_histogram`; an empty prefix produces `native_histogram`.

- common scalar children: `schema`, `zero_threshold`, `sum`, `reset_hint`,
  `start_timestamp`;
- count children: `count_i64` / `zero_count_i64` or `count_f64` / `zero_count_f64`;
- list children for custom values, spans, and positive/negative buckets;
- original Prometheus labels as Greptime tags.

The v2 response always reports written sample, histogram, and exemplar counts in
Prometheus remote-write headers. Exemplars are currently ignored and reported as
zero.
