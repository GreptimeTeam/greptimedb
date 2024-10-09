--- json_path_exists ---
SELECT json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.a');

SELECT json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.c');

SELECT json_path_exists(parse_json('[1, 2]'), '[0]');

SELECT json_path_exists(parse_json('[1, 2]'), '[2]');
