-- EVAL OFFSET: fixed UTC epoch phase (offset + k * interval), deterministic.

CREATE TABLE eval_offset_input (
  ts TIMESTAMP(3) TIME INDEX,
  series STRING,
  v DOUBLE,
  PRIMARY KEY(series)
);

-- Scheduled at odd seconds (1 + 2k): phase anchored to the Unix epoch.
CREATE FLOW eval_offset_phase_flow
SINK TO eval_offset_phase_sink
EVAL INTERVAL '2s'
EVAL OFFSET '1s'
AS
SELECT date_trunc('second', now()) AS ts, count(v) AS c
FROM eval_offset_input
GROUP BY date_trunc('second', now());

-- EVAL INTERVAL without an offset is epoch-anchored: even seconds (2k).
CREATE FLOW eval_zero_offset_phase_flow
SINK TO eval_zero_offset_phase_sink
EVAL INTERVAL '2s'
AS
SELECT date_trunc('second', now()) AS ts, count(v) AS c
FROM eval_offset_input
GROUP BY date_trunc('second', now());

SHOW CREATE FLOW eval_offset_phase_flow;

SELECT flow_definition FROM information_schema.flows WHERE flow_name = 'eval_offset_phase_flow';

-- A zero offset is canonicalized away: SHOW CREATE omits EVAL OFFSET.
CREATE FLOW eval_zero_offset_canonical_flow
SINK TO eval_zero_offset_canonical_sink
EVAL INTERVAL '2s'
EVAL OFFSET '0s'
AS
SELECT date_trunc('second', now()) AS ts, count(v) AS c
FROM eval_offset_input
GROUP BY date_trunc('second', now());

SHOW CREATE FLOW eval_zero_offset_canonical_flow;

SELECT flow_definition FROM information_schema.flows WHERE flow_name = 'eval_zero_offset_canonical_flow';

INSERT INTO eval_offset_input VALUES
  (now(), 'a', 1.0),
  (now(), 'b', 2.0);

-- Let both scheduled flows evaluate at least once.
-- SQLNESS SLEEP 5s

-- Prove the offset phase: every row the offset flow wrote has an odd
-- second-of-minute (1 + 2k is always odd), while the zero-offset flow wrote
-- even seconds (2k is always even). This is deterministic regardless of the
-- exact wall-clock alignment of the evaluations.
SELECT
  count(*) > 0 AS offset_flow_ran,
  bool_and(date_part('second', ts)::BIGINT % 2 = 1) AS offset_flow_at_odd_seconds
FROM eval_offset_phase_sink;

SELECT
  count(*) > 0 AS zero_offset_flow_ran,
  bool_and(date_part('second', ts)::BIGINT % 2 = 0) AS zero_offset_flow_at_even_seconds
FROM eval_zero_offset_phase_sink;

-- Invalid syntax / validation cases.
CREATE FLOW invalid_eval_offset_no_interval
SINK TO invalid_eval_offset_sink
EVAL OFFSET '1s'
AS SELECT 1;

CREATE FLOW invalid_eval_offset_too_large
SINK TO invalid_eval_offset_sink
EVAL INTERVAL '1s'
EVAL OFFSET '2s'
AS SELECT 1;

CREATE FLOW invalid_eval_offset_fractional
SINK TO invalid_eval_offset_sink
EVAL INTERVAL '1s'
EVAL OFFSET '0.5s'
AS SELECT 1;

CREATE FLOW invalid_eval_offset_fractional_interval
SINK TO invalid_eval_offset_sink
EVAL INTERVAL '1.5s'
EVAL OFFSET '1s'
AS SELECT 1;

CREATE FLOW invalid_eval_offset_month
SINK TO invalid_eval_offset_sink
EVAL INTERVAL '1 hour'
EVAL OFFSET '1 month'
AS SELECT 1;

DROP FLOW eval_offset_phase_flow;
DROP FLOW eval_zero_offset_phase_flow;
DROP FLOW eval_zero_offset_canonical_flow;
DROP TABLE eval_offset_phase_sink;
DROP TABLE eval_zero_offset_phase_sink;
DROP TABLE eval_zero_offset_canonical_sink;
DROP TABLE eval_offset_input;
