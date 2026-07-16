SELECT ts, j.a AS a, j.nested.s AS nested_s
FROM t_legacy_json2_non_append_table
ORDER BY ts;

SHOW CREATE TABLE t_legacy_json2_non_append_table;

ALTER TABLE t_legacy_json2_non_append_table SET 'append_mode' = 'true';

SHOW CREATE TABLE t_legacy_json2_non_append_table;

INSERT INTO t_legacy_json2_non_append_table (ts, j) VALUES
  ('2026-07-08 00:02:00+0000', '{"a": 3, "nested": {"s": "current-3"}}');

SELECT ts, j.a AS a, j.nested.s AS nested_s
FROM t_legacy_json2_non_append_table
ORDER BY ts;
