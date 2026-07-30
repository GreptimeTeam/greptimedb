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

--- json_object_keys ---

SELECT json_object_keys(parse_json('{"z": 1, "nested": {"x": 2}, "a": 3}'));

SELECT object_keys(parse_json('{"b": 2, "a": 1}'));

SELECT json_object_keys(parse_json('{}'));

SELECT json_object_keys(NULL);

SELECT json_object_keys(parse_json('null'));

SELECT json_object_keys(parse_json('[1, 2]'));

SELECT json_object_keys(parse_json('"text"'));

SELECT json_object_keys(parse_json('true'));

SELECT json_object_keys(parse_json('42'));

SELECT json_object_keys(parse_json('1.5'));

SELECT json_object_keys(parse_json('{"中": 1, "quote\"key": 2, "\u0061 b": 3}'));
