SELECT COUNT(*) AS n FROM t_trace_v2;

SELECT COUNT(*) AS n FROM t_trace_v2
WHERE span_attributes."http.status_code"::BIGINT = 200
  AND span_attributes.nested."a.b"[0]::BOOLEAN = true;

SELECT COUNT(*) AS n FROM t_trace_v2
WHERE span_attributes."http.status_code"::STRING = 'ok'
  AND span_attributes.bytes[1]::BIGINT = 2
  AND scope_attributes.enabled::BOOLEAN = true
  AND resource_attributes."deployment.environment"::STRING = 'prod';

SELECT COUNT(*) AS n FROM information_schema.columns
WHERE table_name = 't_trace_v2';

SELECT COUNT(*) AS n FROM information_schema.tables
WHERE table_name = 't_trace_v2'
  AND create_options LIKE '%table_data_model=greptime_trace_v2%'
  AND create_options LIKE '%greptime.semantic.pipeline=greptime_trace_v2%';

SELECT COUNT(*) AS n FROM t_trace_v2
WHERE json_get_string(span_events, '$[0].name') = 'cache.hit'
  AND json_get_int(span_events, '$[0].attributes."event.code"') = 7
  AND json_get_string(span_links, '$[0].trace_id') = 'linked-trace'
  AND json_get_string(span_links, '$[0].attributes."link.type"') = 'follows_from';

SELECT COUNT(*) AS n FROM t_trace_v2
WHERE json_to_string(span_events) = '[]' AND json_to_string(span_links) = '[]';
