CREATE TABLE t_json2_compat(
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    j JSON
);

INSERT INTO t_json2_compat VALUES
('2024-02-05 00:00:00+0000', 'host_a', parse_json('{"a": 10, "nested": {"b": "x"}, "flag": true}')),
('2024-02-05 00:01:00+0000', 'host_b', parse_json('{"a": 20, "nested": {"b": "y"}, "flag": false}')),
('2024-02-05 00:02:00+0000', 'host_c', parse_json('{"a": 30, "nested": {"b": "z"}, "flag": true}'));

ADMIN FLUSH_TABLE('t_json2_compat');
