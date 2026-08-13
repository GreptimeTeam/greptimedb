-- Runs on the OLD binary (no EVAL OFFSET support): create an `EVAL INTERVAL`
-- flow without any offset. The old binary persists metadata without a typed
-- eval_schedule; the new binary must recover it with an implicit zero offset.
CREATE TABLE compat_eval_offset_old_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  v DOUBLE,
  PRIMARY KEY(series)
);

CREATE FLOW compat_eval_offset_old_flow
SINK TO compat_eval_offset_old_sink
EVAL INTERVAL '1s'
AS
SELECT
  date_trunc('second', now()) AS ts,
  count(v) AS value_count
FROM compat_eval_offset_old_input
GROUP BY date_trunc('second', now());
