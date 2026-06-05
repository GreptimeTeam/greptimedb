DESC TABLE information_schema.table_semantics;

CREATE TABLE metrics_tagged (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
)
WITH (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.pipeline' = 'greptime_metric_v1',
  'greptime.semantic.metric.metadata_quality' = 'declared',
  'greptime.semantic.metric.type' = 'counter',
  'greptime.semantic.metric.unit' = 'By'
);

CREATE TABLE traces_tagged (
  ts TIMESTAMP TIME INDEX,
  span_name STRING,
)
WITH (
  'greptime.semantic.signal_type' = 'trace',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.trace.conventions' = 'https://opentelemetry.io/schemas/1.27.0'
);

-- A table with no semantic options must not appear in the view.
CREATE TABLE plain_table (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
);

SELECT table_schema, table_name, signal_type, source, pipeline, metadata_quality, semantic_options
FROM information_schema.table_semantics
ORDER BY table_name;

-- Predicate pushdown on a promoted column.
SELECT table_name, signal_type
FROM information_schema.table_semantics
WHERE signal_type = 'metric'
ORDER BY table_name;

DROP TABLE metrics_tagged;

DROP TABLE traces_tagged;

DROP TABLE plain_table;
