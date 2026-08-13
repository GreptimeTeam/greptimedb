-- Runs on the OLD binary (no EVAL OFFSET support): an `EVAL INTERVAL` flow
-- over an instant-TTL source is forced to the streaming engine, which persists
-- flow_type=streaming together with eval_interval in the metadata. A new binary
-- must reject this metadata on recovery instead of silently routing it to the
-- streaming engine (which would discard the schedule) or migrating it.
CREATE TABLE compat_eval_interval_streaming_old_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  v DOUBLE,
  PRIMARY KEY(series)
) WITH ('ttl' = 'instant');

CREATE FLOW compat_eval_interval_streaming_old_flow
SINK TO compat_eval_interval_streaming_old_sink
EVAL INTERVAL '1s'
AS
SELECT
  date_trunc('second', now()) AS ts,
  count(v) AS value_count
FROM compat_eval_interval_streaming_old_input
GROUP BY date_trunc('second', now());
