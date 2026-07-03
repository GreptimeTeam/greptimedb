CREATE TABLE compat_flow_now_input (
  ts TIMESTAMP(3) TIME INDEX,
  v DOUBLE,
  PRIMARY KEY(v)
);

CREATE FLOW compat_flow_scheduled_now
SINK TO compat_flow_now_sink
EVAL INTERVAL '1s'
AS
SELECT
  date_bin(INTERVAL '1 second', ts) AS window_start,
  count(v) AS value_count,
  now() AS create_time,
  current_timestamp() AS cur_ts
FROM compat_flow_now_input
WHERE ts >= date_trunc('second', now()) - INTERVAL '1 second'
  AND ts <  date_trunc('second', current_timestamp())
GROUP BY date_bin(INTERVAL '1 second', ts);
