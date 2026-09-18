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
