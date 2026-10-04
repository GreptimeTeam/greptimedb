CREATE TABLE t_json2_storage_layout_settings (
  ts TIMESTAMP TIME INDEX,
  j JSON2(max_auto_expanded_paths = 0)
) WITH (
  'append_mode' = 'true'
);

INSERT INTO t_json2_storage_layout_settings VALUES
  ('2026-08-17 00:00:00+0000', '{"a": 1}');

ADMIN FLUSH_TABLE('t_json2_storage_layout_settings');
