--- json_object_keys ---
SELECT json_object_keys(parse_json('{"a": 1, "b": {"c": 2}}'));

SELECT json_object_keys(parse_json('{}'));

SELECT json_object_keys(parse_json('[1, 2]'));

SELECT json_object_keys(parse_json('null'));

SELECT json_object_keys(NULL);

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

--- json_object ---
SELECT json_to_string(json_object('a', 1, 'b', 'text', 'c', true, 'd', 1.5));

SELECT json_to_string(json_object());

SELECT json_to_string(json_object('nul', NULL));

SELECT json_to_string(json_object('nl', concat('line1', chr(10), 'line2'), 'q', 'quote"back\slash'));

SELECT json_object('a');

SELECT json_object(NULL, 1);

SELECT json_object('ts', to_timestamp(0));

SELECT json_object('price', CAST('12.34' AS DECIMAL(10, 2)));

CREATE TABLE json_object_src (ts TIMESTAMP TIME INDEX, host STRING, pid BIGINT);

INSERT INTO json_object_src VALUES (0, 'h1', 42), (1, NULL, 7);

SELECT json_to_string(json_object('host', host, 'pid', pid)) FROM json_object_src ORDER BY ts;

DROP TABLE json_object_src;
