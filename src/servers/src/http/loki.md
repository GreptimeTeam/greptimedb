# Loki Push Ingestion

This note describes the HTTP Loki push path implemented in `loki.rs`.

## Entry Point

The route is registered by `HttpServer::route_loki`:

```text
POST /v1/loki/api/v1/push
```

The handler is `loki_ingest`. It receives the authenticated `QueryContext`, marks
the channel and semantic source as Loki log ingestion, resolves the target table,
and converts the request body into a `ContextReq`.

The target table is selected by `x-greptime-log-table-name`. If the header is
absent, Loki writes to `loki_logs`.

## Supported Payloads

Payload decoding is selected by `Content-Type`.

| Content-Type | Parser | Notes |
| --- | --- | --- |
| `application/json` | `LokiJsonParser` | Expects a top-level `streams` array. |
| `application/x-protobuf` | `LokiPbParser` | Expects a Snappy-compressed Loki `PushRequest`. |

The HTTP route also has request decompression middleware. That is separate from
the Snappy decoding required by Loki protobuf push bodies.

Both parsers produce `LokiMiddleItem` values:

```text
JSON stream values  -> LokiMiddleItem<VrlValue>
protobuf entries    -> LokiMiddleItem<Vec<LabelPairAdapter>>
```

Malformed inner streams or entries are warned and skipped. Invalid top-level
payloads, unsupported content types, protobuf decode errors, and protobuf Snappy
decode errors return request errors.

## Direct Ingestion

When no pipeline header is present, Loki uses the direct-write path:

```text
bytes -> extract_item<LokiRawItem> -> RowInsertRequest -> ContextReq
```

The initial schema is:

| Column | Type | Semantic type |
| --- | --- | --- |
| `greptime_timestamp()` | timestamp nanosecond | timestamp |
| `line` | string | field |
| `structured_metadata` | JSON binary | field |

Loki labels are added as string tag columns. Since later entries may introduce
new labels, previously built rows are padded with nulls after the final schema is
known.

Structured metadata is stored as one JSON binary value in the
`structured_metadata` field.

## Pipeline Ingestion

When `x-greptime-log-pipeline-name` or `x-greptime-pipeline-name` is present,
Loki uses the pipeline path:

```text
bytes -> extract_item<LokiPipeline> -> PipelineIngestRequest -> run_pipeline -> ContextReq
```

Pipeline version and parameters come from the shared pipeline headers:

| Header | Meaning |
| --- | --- |
| `x-greptime-log-pipeline-version` / `x-greptime-pipeline-version` | Pipeline version. |
| `x-greptime-pipeline-params` | Pipeline parameters. |

Each Loki entry is converted into a VRL object before pipeline execution:

| Source | Pipeline field |
| --- | --- |
| Entry timestamp | `greptime_timestamp()` |
| Entry line | `loki_line` |
| Structured metadata key `k` | `loki_metadata_k` |
| Label key `k` | `loki_label_k` |

## Execution And Metrics

Both direct and pipeline paths produce a `ContextReq`. The shared
`execute_log_context_req` helper performs inserts through `PipelineHandler::insert`,
collects all outputs, builds a `GreptimedbV1Response`, and records aggregate
Loki ingestion metrics.

The elapsed timer starts before payload conversion, so it includes parsing,
optional pipeline execution, and insertion.

## Important Contracts

- The public endpoint and response format are unchanged from the Loki push API.
- `LogState.log_validator` is not used by this Loki-specific handler.
- Protobuf push bodies must be Snappy-compressed even when there is no HTTP
  `Content-Encoding`.
- Invalid Snappy data must return a controlled Loki decompression error rather
  than panic.
