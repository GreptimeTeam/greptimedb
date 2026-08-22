-- Runs on the NEW binary: old interval-flow metadata must recover with an
-- implicit zero offset (no `EVAL OFFSET` line) and no internal transport keys.
SELECT flow_name, source_table_names
FROM information_schema.flows
WHERE flow_name = 'compat_eval_offset_old_flow';

SHOW CREATE FLOW compat_eval_offset_old_flow;

SELECT flow_definition
FROM information_schema.flows
WHERE flow_name = 'compat_eval_offset_old_flow';

-- The recovered flow_definition must not surface internal transport keys.
SELECT count(*) AS leaked_internal_keys
FROM information_schema.flows
WHERE flow_name = 'compat_eval_offset_old_flow'
  AND (flow_definition LIKE '%__greptime_internal%'
       OR flow_definition LIKE '%eval_schedule%');

-- The old flow still executes on the new binary.
INSERT INTO compat_eval_offset_old_input VALUES
  ('2026-06-25 00:00:00', 'a', 1.0),
  ('2026-06-25 00:00:01', 'b', 2.0);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('compat_eval_offset_old_flow');

SELECT count(*) > 0 AS old_flow_executed
FROM compat_eval_offset_old_sink;
