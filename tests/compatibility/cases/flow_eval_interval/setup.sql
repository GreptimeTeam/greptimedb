CREATE TABLE compat_sql_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  v DOUBLE,
  PRIMARY KEY(series)
);

CREATE FLOW compat_sql_eval_flow
SINK TO compat_sql_eval_sink
EVAL INTERVAL '1s'
AS
SELECT
  date_trunc('second', now()) AS ts,
  count(v) AS value_count
FROM compat_sql_input
GROUP BY date_trunc('second', now());

CREATE TABLE compat_tql_input (
  ts TIMESTAMP(3) TIME INDEX,
  host STRING,
  idc STRING,
  val DOUBLE,
  PRIMARY KEY(host, idc)
);

CREATE FLOW compat_tql_eval_flow
SINK TO compat_tql_eval_sink
EVAL INTERVAL '1s'
AS
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", compat_tql_input);
