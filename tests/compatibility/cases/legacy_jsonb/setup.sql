CREATE TABLE legacy_jsonb (
  ts TIMESTAMP TIME INDEX,
  host STRING PRIMARY KEY,
  j JSON
);

INSERT INTO legacy_jsonb (ts, host, j) VALUES
  ('2024-01-01 00:00:01+0000', 'r1', parse_json('{"a":10,"msg":"old-jsonb"}')),
  ('2024-01-01 00:00:02+0000', 'r2', parse_json('{"a":20,"msg":"old-jsonb"}')),
  ('2024-01-01 00:00:03+0000', 'r3', parse_json('{"msg":"no-a"}'));

ADMIN FLUSH_TABLE('legacy_jsonb');
