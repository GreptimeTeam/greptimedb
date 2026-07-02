SELECT flow_name, source_table_names
FROM information_schema.flows
WHERE flow_name = 'compat_flow_scheduled_now';

SHOW CREATE FLOW compat_flow_scheduled_now;

SHOW CREATE TABLE compat_flow_now_sink;

INSERT INTO compat_flow_now_input VALUES
  (date_trunc('second', now()) - INTERVAL '1 second', 0.0),
  (date_trunc('second', now()), 1.0),
  (date_trunc('second', now()) + INTERVAL '1 second', 2.0),
  (date_trunc('second', now()) + INTERVAL '2 seconds', 3.0),
  (date_trunc('second', now()) + INTERVAL '3 seconds', 4.0),
  (date_trunc('second', now()) + INTERVAL '4 seconds', 5.0),
  (date_trunc('second', now()) + INTERVAL '5 seconds', 6.0),
  (date_trunc('second', now()) + INTERVAL '6 seconds', 7.0),
  (date_trunc('second', now()) + INTERVAL '7 seconds', 8.0);

-- SQLNESS SLEEP 4s
SELECT
  count(DISTINCT window_start) >= 1 AS has_selected_window,
  min(value_count) AS min_value_count,
  max(value_count) AS max_value_count,
  bool_and(create_time = date_trunc('second', create_time)) AS all_create_time_at_second_boundary,
  bool_and(cur_ts = create_time) AS cur_ts_equals_create_time,
  bool_and(window_start = create_time - INTERVAL '1 second') AS all_windows_match_scheduled_previous_second
FROM compat_flow_now_sink
WHERE value_count > 0;

SELECT
  bool_and(value_count = 1) AS all_value_count_equals_one
FROM compat_flow_now_sink
WHERE value_count > 0;
