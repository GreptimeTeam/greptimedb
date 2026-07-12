CREATE TABLE scheduled_late_overlap_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  reading DOUBLE,
  PRIMARY KEY(series)
);

CREATE FLOW scheduled_late_overlap_flow
SINK TO scheduled_late_overlap_sink
EVAL INTERVAL '1s'
AS
WITH
target_offsets(delta) AS (
  VALUES
    (INTERVAL '1 second'),
    (INTERVAL '2 seconds'),
    (INTERVAL '3 seconds'),
    (INTERVAL '4 seconds'),
    (INTERVAL '5 seconds'),
    (INTERVAL '6 seconds'),
    (INTERVAL '7 seconds'),
    (INTERVAL '8 seconds'),
    (INTERVAL '9 seconds'),
    (INTERVAL '10 seconds')
),
target_seconds AS (
  SELECT date_trunc('second', now()) - delta AS target_ts
  FROM target_offsets
),
bucketed AS (
  SELECT
    series,
    date_bin(INTERVAL '1 second', ts) AS bucket_ts,
    last_value(reading ORDER BY ts) AS reading
  FROM scheduled_late_overlap_input
  WHERE ts >= date_trunc('second', now()) - INTERVAL '20 seconds'
    AND ts <  date_trunc('second', now())
  GROUP BY series, date_bin(INTERVAL '1 second', ts)
)
SELECT
  target_seconds.target_ts AS ts,
  bucketed.series,
  bucketed.reading,
  now() AS create_time
FROM target_seconds
JOIN bucketed
  ON bucketed.bucket_ts = target_seconds.target_ts;

INSERT INTO scheduled_late_overlap_input VALUES
  (date_trunc('second', now()), 'seed', 1.0);

-- SQLNESS SLEEP 3s
INSERT INTO scheduled_late_overlap_input
SELECT min(ts), 'late_a', 10.0 FROM scheduled_late_overlap_input
UNION ALL
SELECT min(ts), 'late_b', 11.0 FROM scheduled_late_overlap_input;

-- SQLNESS SLEEP 4s
SELECT
  count(DISTINCT series) >= 3 AS replayed_late_series,
  bool_and(ts < create_time) AS all_ts_before_create_time,
  bool_and(ts >= create_time - INTERVAL '10 seconds') AS all_ts_within_overlap_window
FROM scheduled_late_overlap_sink
WHERE ts = (SELECT min(ts) FROM scheduled_late_overlap_input);

DROP FLOW scheduled_late_overlap_flow;
DROP TABLE scheduled_late_overlap_sink;
DROP TABLE scheduled_late_overlap_input;
