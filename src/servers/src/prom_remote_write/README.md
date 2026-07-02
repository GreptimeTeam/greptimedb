# Prometheus Remote Write

This module decodes Prometheus remote write requests and converts them into
Greptime row insert requests.

## Remote Write V2

Remote write v2 enters through `remote_write_v2` in
`src/servers/src/http/prom_store.rs`.

```mermaid
flowchart TD
    A["HTTP /v1/prometheus/write"] --> B["remote_write_v2"]
    B --> C["decode_remote_write_v2_request"]
    C --> D["into_write_requests"]

    D --> E["samples ContextReq"]
    D --> F["histograms ContextReq"]

    E --> G["write_prometheus_rows_with_progress"]
    G --> H["metric engine / pending batcher when enabled"]

    F --> I["histogram ContextOpt"]
    I --> J["write_prometheus_rows_with_progress"]
    J --> K["same metric-engine flag as samples, no batcher"]
    K --> L["table: <metric>"]

    L --> M["field: greptime_native_histogram Struct"]
    M --> N["struct children: counts, spans, buckets, sum, schema"]
    H --> P["written headers and counters"]
    K --> P
```

The conversion step splits one v2 request into two `ContextReq`s:

- samples keep the existing sample table name and can use the metric-engine
  physical table path and pending rows batcher;
- native histograms keep the existing metric table name.

Native histogram rows follow the same metric-engine switch as samples. They do
not use the pending rows batcher yet because the batcher assumes the classic
timestamp + Float64 value + string tags shape.

Each histogram row stores `greptime_native_histogram` as one Struct field:

- common scalar children: `schema`, `zero_threshold`, `sum`, `reset_hint`,
  `start_timestamp`;
- count children: `count_u64` / `zero_count_u64` or `count_f64` / `zero_count_f64`;
- list children for custom values, spans, and positive/negative buckets;
- original Prometheus labels as Greptime tags.

The v2 response always reports written sample, histogram, and exemplar counts in
Prometheus remote-write headers. Exemplars are currently ignored and reported as
zero.
