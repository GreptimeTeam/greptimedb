CREATE TABLE t_legacy_json2_non_append_table (
  ts TIMESTAMP TIME INDEX,
  j JSON2
);

INSERT INTO t_legacy_json2_non_append_table (ts, j) VALUES
  ('2026-07-08 00:00:00+0000', '{"a": 1, "nested": {"s": "old-1"}}'),
  ('2026-07-08 00:01:00+0000', '{"a": 2, "nested": {"s": "old-2"}}');

ADMIN FLUSH_TABLE('t_legacy_json2_non_append_table');
