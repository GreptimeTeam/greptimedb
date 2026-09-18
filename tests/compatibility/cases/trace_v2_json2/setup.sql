CREATE TABLE t_trace_v2 (
    "timestamp" TIMESTAMP(9) TIME INDEX,
    timestamp_end TIMESTAMP(9),
    duration_nano BIGINT,
    parent_span_id STRING,
    trace_id STRING,
    span_id STRING,
    span_kind STRING,
    span_name STRING,
    span_status_code STRING,
    span_status_message STRING,
    trace_state STRING,
    scope_name STRING,
    scope_version STRING,
    service_name STRING PRIMARY KEY,
    span_attributes JSON2,
    scope_attributes JSON2,
    resource_attributes JSON2,
    span_events JSON,
    span_links JSON
) WITH (
    'append_mode' = 'true',
    'table_data_model' = 'greptime_trace_v2',
    'greptime.semantic.pipeline' = 'greptime_trace_v2',
    'greptime.semantic.signal_type' = 'trace',
    'greptime.semantic.source' = 'opentelemetry',
    'greptime.semantic.entity.service.id' = 'service_name',
    'greptime.semantic.trace.conventions' = 'unknown'
);

INSERT INTO t_trace_v2 (
    "timestamp", timestamp_end, duration_nano, service_name, trace_id, span_id,
    span_attributes, scope_attributes, resource_attributes, span_events, span_links
) VALUES
    (1, 11, 10, NULL, 'trace-1', 'span-1',
     '{"http.status_code":200,"nested":{"a.b":[true,"ok"]}}', '{}', '{}',
     '[{"name":"cache.hit","time":"2025-01-10 03:49:02.444400+0000","attributes":{"event.code":7}}]',
     '[{"trace_id":"linked-trace","span_id":"linked-span","trace_state":"vendor=value","attributes":{"link.type":"follows_from"}}]'),
    (1, 21, 20, NULL, 'trace-2', 'span-2',
     '{"http.status_code":"ok","bytes":[1,2,3]}',
     '{"enabled":true}', '{"deployment.environment":"prod"}', '[]', '[]');

ADMIN FLUSH_TABLE('t_trace_v2');
