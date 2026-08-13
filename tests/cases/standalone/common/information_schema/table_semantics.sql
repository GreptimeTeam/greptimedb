DESC TABLE information_schema.table_semantics;

CREATE TABLE metrics_tagged (
  ts TIMESTAMP TIME INDEX,
  val DOUBLE,
)
WITH (
  'greptime.semantic.signal_type' = 'metric',
  'greptime.semantic.source' = 'opentelemetry',
  'greptime.semantic.source_version' = '2.0',
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

SELECT table_schema, table_name, signal_type, source, source_version, pipeline, metadata_quality, semantic_options
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

-- ALTER TABLE manages semantic declarations on existing tables: SET appears in
-- the view, UNSET disappears from it.
CREATE TABLE altered_semantics (
  ts TIMESTAMP TIME INDEX,
  svc STRING,
  payload BINARY,
  val DOUBLE,
);

ALTER TABLE altered_semantics SET 'greptime.semantic.signal_type' = 'metric', 'greptime.semantic.entity.service.id' = 'svc';

SELECT table_name, signal_type, semantic_options
FROM information_schema.table_semantics
WHERE table_name = 'altered_semantics';

-- Semantic options never share a statement with regular table options.
ALTER TABLE altered_semantics SET 'greptime.semantic.source' = 'prometheus', 'ttl' = '7d';

-- Unknown semantic keys are rejected on SET.
ALTER TABLE altered_semantics SET 'greptime.semantic.nonsense' = 'x';

-- Values outside the key's domain are rejected.
ALTER TABLE altered_semantics SET 'greptime.semantic.signal_type' = 'garbage';

-- Entity columns must exist.
ALTER TABLE altered_semantics SET 'greptime.semantic.entity.host.id' = 'no_such_column';

-- Entity columns must render as strings.
ALTER TABLE altered_semantics SET 'greptime.semantic.entity.host.id' = 'payload';

ALTER TABLE altered_semantics UNSET 'greptime.semantic.entity.service.id';

SELECT table_name, signal_type, semantic_options
FROM information_schema.table_semantics
WHERE table_name = 'altered_semantics';

DROP TABLE altered_semantics;

-- Logical metric tables take the same metadata-only path.
CREATE TABLE phy_sem (ts TIMESTAMP TIME INDEX, val DOUBLE) engine=metric with ("physical_metric_table" = "");

CREATE TABLE logical_sem (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) engine=metric with ("on_physical_table" = "phy_sem");

ALTER TABLE logical_sem SET 'greptime.semantic.signal_type' = 'metric', 'greptime.semantic.entity.host.id' = 'host';

SELECT table_name, signal_type, semantic_options
FROM information_schema.table_semantics
WHERE table_name = 'logical_sem';

-- Regular options still cannot be altered on logical tables.
ALTER TABLE logical_sem SET 'ttl' = '7d';

ALTER TABLE logical_sem UNSET 'greptime.semantic.entity.host.id';

SELECT table_name, signal_type, semantic_options
FROM information_schema.table_semantics
WHERE table_name = 'logical_sem';

DROP TABLE logical_sem;

DROP TABLE phy_sem;
