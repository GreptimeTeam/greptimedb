-- Regression for a TQL flow whose pre-created sink table is missing the value
-- output column. The labels are intentionally minimal and anonymous.

CREATE DATABASE source_schema;
CREATE DATABASE sink_schema;

USE source_schema;

CREATE TABLE metric_input (
  namespace STRING NULL,
  app STRING NULL,
  greptime_timestamp TIMESTAMP(3) NOT NULL,
  greptime_value DOUBLE NULL,
  TIME INDEX (greptime_timestamp),
  PRIMARY KEY (namespace, app)
);

INSERT INTO metric_input VALUES
  ('ns', 'app-a', '2026-01-23T03:40:00Z', 10.0),
  ('ns', 'app-a', '2026-01-23T03:50:00Z', 20.0);

USE sink_schema;

-- Intentionally omit greptime_value DOUBLE from the pre-created sink table.
CREATE TABLE missing_value_sink (
  namespace STRING NULL,
  app STRING NULL,
  greptime_timestamp TIMESTAMP(3) NOT NULL,
  TIME INDEX (greptime_timestamp),
  PRIMARY KEY (namespace, app)
)
ENGINE=mito;

-- SQLNESS REPLACE (in\scontext:\sFailed\sto\srewrite\splan:\sError\sduring\splanning:.*) in context: Failed to rewrite plan
CREATE FLOW missing_value_flow
SINK TO sink_schema.missing_value_sink
EVAL INTERVAL '3600 s'
AS TQL EVAL (
  date_bin('2m'::interval, now() - '2m'::interval),
  date_bin('2m'::interval, now() - '2m'::interval),
  '1h'
)
  avg by (namespace, app) (
    avg_over_time(metric_input{__schema__="source_schema"}[1h])
  );

DROP FLOW IF EXISTS missing_value_flow;
DROP TABLE missing_value_sink;

USE source_schema;
DROP TABLE metric_input;

USE public;
DROP DATABASE sink_schema;
DROP DATABASE source_schema;
