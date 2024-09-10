-- get_by_path functions --
SELECT get_by_path_int(to_json('{"a": {"b": {"c": 1}}}'), 'a.b.c');

SELECT get_by_path_float(to_json('{"a": {"b": {"c": 1.234}}}'), 'a.b.c');

SELECT get_by_path_string(to_json('{"a": {"b": {"c": "foo"}}}'), 'a.b.c');

SELECT get_by_path_bool(to_json('{"a": {"b": {"c": true}}}'), 'a.b.c');

SELECT get_by_path_int(to_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

SELECT get_by_path_string(to_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

-- test functions with table rows --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": 1}}}'), 1);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": 1.234}}}'), 2);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": "foo"}}}'), 3);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": true}}}'), 4);

SELECT get_by_path_int(j, 'a.b.c') FROM jsons;

SELECT get_by_path_float(j, 'a.b.c') FROM jsons;

SELECT get_by_path_string(j, 'a.b.c') FROM jsons;

SELECT get_by_path_bool(j, 'a.b.c') FROM jsons;

SELECT get_by_path_int(j, 'd') FROM jsons;

DROP TABLE jsons;
