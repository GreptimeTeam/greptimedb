CREATE TABLE eval_interval_schedule_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  v DOUBLE,
  PRIMARY KEY(series)
);

CREATE FLOW eval_interval_schedule_flow
SINK TO eval_interval_schedule_sink
EVAL INTERVAL '1s'
AS
SELECT
  date_trunc('second', now()) AS ts,
  now() AS create_time,
  current_timestamp() AS cur_ts,
  count(v) AS value_count
FROM eval_interval_schedule_input
GROUP BY date_trunc('second', now());

INSERT INTO eval_interval_schedule_input VALUES
  ('2026-06-25 00:00:00', 'a', 1.0);

-- SQLNESS SLEEP 5s
SELECT
  count(DISTINCT ts) >= 2 AS has_multiple_scheduled_ticks,
  min(value_count) AS min_value_count,
  max(value_count) AS max_value_count,
  bool_and(create_time = date_trunc('second', create_time)) AS all_create_time_at_second_boundary,
  bool_and(create_time = ts) AS create_time_equals_ts,
  bool_and(cur_ts = date_trunc('second', cur_ts)) AS all_cur_ts_at_second_boundary,
  bool_and(cur_ts = create_time) AS cur_ts_equals_create_time
FROM eval_interval_schedule_sink
WHERE value_count > 0;

DROP FLOW eval_interval_schedule_flow;
DROP TABLE eval_interval_schedule_sink;
DROP TABLE eval_interval_schedule_input;
