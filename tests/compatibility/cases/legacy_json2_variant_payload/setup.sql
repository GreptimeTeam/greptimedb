CREATE TABLE t_legacy_json2_variant_payload (
  ts TIMESTAMP TIME INDEX,
  j JSON2
) WITH (
  'append_mode' = 'true'
);

INSERT INTO t_legacy_json2_variant_payload (ts, j) VALUES
  ('2026-07-08 00:00:00+0000', '{"a": {"b": 1}, "c": "s1", "d": [{"e": {"f": 0.1}}]}'),
  ('2026-07-08 00:01:00+0000', '{"a": {"b": -2}, "c": [1], "d": [{"e": {"f": 0.2}}]}'),
  ('2026-07-08 00:02:00+0000', '{"a": {"b": "s3"}, "c": "s3", "d": [{"e": {"g": -0.3}}]}'),
  ('2026-07-08 00:03:00+0000', '{"a": {"b": null}, "c": null}'),
  ('2026-07-08 00:04:00+0000', '{"c": "s5", "d": {"e": true}}'),
  ('2026-07-08 00:05:00+0000', '{"a": {"b": 6}}');

ADMIN FLUSH_TABLE('t_legacy_json2_variant_payload');

CREATE TABLE t_legacy_json2_variant_payload_object_d (
  ts TIMESTAMP TIME INDEX,
  j JSON2
) WITH (
  'append_mode' = 'true'
);

INSERT INTO t_legacy_json2_variant_payload_object_d (ts, j) VALUES
  ('2026-07-08 01:00:00+0000', '{"d": {"e": true}}'),
  ('2026-07-08 01:01:00+0000', '{"d": {"e": false}}');

ADMIN FLUSH_TABLE('t_legacy_json2_variant_payload_object_d');
