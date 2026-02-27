--- json_path_exists ---
SELECT json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.a');

SELECT json_path_exists(parse_json('{"a": 1, "b": 2}'), '$.c');

SELECT json_path_exists(parse_json('[1, 2]'), '[0]');

SELECT json_path_exists(parse_json('[1, 2]'), '[2]');

SELECT json_path_exists(parse_json('[1, 2]'), 'null');

SELECT json_path_exists(parse_json('null'), '$.a');

SELECT json_path_exists(NULL, '$.a');

SELECT json_path_exists(parse_json('{}'), NULL);

--- json_path_match ---

SELECT json_path_match(parse_json('{"a": 1, "b": 2}'), '$.a == 1');

SELECT json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[0] > 1');

SELECT json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), '$.b[1 to last] >= 2');

SELECT json_path_match(parse_json('{"a":1,"b":[1,2,3]}'), 'null');

SELECT json_path_match(parse_json('null'), '$.a == 1');

--- json path query ---

SELECT json_to_string(json_path_query(parse_json('{"a": 1, "b": 2}'), '$.a'));

SELECT json_to_string(json_path_query(parse_json('{"a": 1, "b": 2}'), '$.*'));

SELECT json_to_string(json_path_query(parse_json('{"a": 1, "b": 2}'), '$.* ? (@ > 1)'));

SELECT json_to_string(json_path_query(parse_json('{"a": 1, "b": 2}'), '$.a.type()'));
