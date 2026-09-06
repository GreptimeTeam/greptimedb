CREATE TABLE compat_eval_offset_input (
  ts TIMESTAMP(3) TIME INDEX,
  v DOUBLE
);

CREATE FLOW compat_eval_offset_flow
SINK TO compat_eval_offset_sink
EVAL INTERVAL '10s'
EVAL OFFSET '3s'
AS
SELECT
  date_trunc('second', now()) AS ts,
  count(v) AS value_count
FROM compat_eval_offset_input
GROUP BY date_trunc('second', now());
