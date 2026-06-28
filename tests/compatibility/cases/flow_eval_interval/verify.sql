SELECT flow_name, source_table_names
FROM information_schema.flows
WHERE flow_name IN ('compat_sql_eval_flow', 'compat_tql_eval_flow')
ORDER BY flow_name;

SHOW CREATE TABLE compat_sql_eval_sink;

SHOW CREATE TABLE compat_tql_eval_sink;

INSERT INTO compat_sql_input VALUES
  ('2026-06-25 00:00:00', 'a', 1.0),
  ('2026-06-25 00:00:01', 'b', 2.0);

INSERT INTO compat_tql_input VALUES
  (now() - '17s'::interval, 'host1', 'idc1', 200),
  (now() - '13s'::interval, 'host2', 'idc1', 401),
  (now() - '7s'::interval, 'host3', 'idc2', 500);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('compat_sql_eval_flow');

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('compat_tql_eval_flow');

SELECT count(*) > 0 AS sql_flow_executed
FROM compat_sql_eval_sink;

SELECT count(*) > 0 AS tql_flow_executed
FROM compat_tql_eval_sink;
