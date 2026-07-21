CREATE TABLE compat_comment_table (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING PRIMARY KEY,
  val DOUBLE
);

CREATE TABLE compat_comment_flow_source (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY(host)
);

CREATE TABLE compat_comment_flow_sink (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING,
  val DOUBLE,
  PRIMARY KEY(host)
);

CREATE FLOW compat_comment_flow
SINK TO compat_comment_flow_sink
AS
SELECT ts, host, val FROM compat_comment_flow_source;
