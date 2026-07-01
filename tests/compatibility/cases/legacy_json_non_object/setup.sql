CREATE TABLE t_legacy_json_non_object(
    ts TIMESTAMP TIME INDEX,
    kind STRING PRIMARY KEY,
    j JSON
);

INSERT INTO t_legacy_json_non_object VALUES
('2024-02-06 00:00:00+0000', 'array', parse_json('[1, 2, 3]')),
('2024-02-06 00:01:00+0000', 'string', parse_json('"hello"')),
('2024-02-06 00:02:00+0000', 'int', parse_json('42')),
('2024-02-06 00:03:00+0000', 'bool', parse_json('true')),
('2024-02-06 00:04:00+0000', 'null', parse_json('null')),
('2024-02-06 00:05:00+0000', 'object', parse_json('{"a": 10, "msg": "object"}'));

ADMIN FLUSH_TABLE('t_legacy_json_non_object');
