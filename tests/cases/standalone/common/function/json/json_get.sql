-- json_get functions --
SELECT json_get_int(parse_json('{"a": {"b": {"c": 1}}}'), 'a.b.c');

SELECT json_get_float(parse_json('{"a": {"b": {"c": 1.234}}}'), 'a:b.c');

SELECT json_get_string(parse_json('{"a": {"b": {"c": "foo"}}}'), 'a.b:c');

SELECT json_get_bool(parse_json('{"a": {"b": {"c": true}}}'), 'a.b["c"]');

SELECT json_get_int(parse_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

SELECT json_get_string(parse_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

SELECT json_to_string(json_get_object(parse_json('{"a": {"b": {"c": {"d": 42}}}}'), 'a.b.c'));

SELECT json_get_int(json_get_object(parse_json('{"a": {"b": {"c": {"d": 42}}}}'), 'a.b.c'), 'd');

SELECT json_get_object(parse_json('{"a": {"b": {"c": {"d": 42}}}}'), 'a.e');

-- test functions with table rows --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": 1}}}'), 1);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": 1.234}}}'), 2);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": "foo"}}}'), 3);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": true}}}'), 4);

SELECT json_get_int(j, 'a.b.c') FROM jsons;

SELECT json_get_float(j, 'a["b"].c') FROM jsons;

SELECT json_get_string(j, 'a.b.c?(@ == 1)') FROM jsons;

SELECT json_get_bool(j, 'a.b.c') FROM jsons;

SELECT json_get_int(j, 'a.b["c"]') FROM jsons;

SELECT json_to_string(json_get_object(j, 'a.b')) FROM jsons;

SELECT json_get_string(json_get_object(j, 'a.b'), 'c') FROM jsons;

SELECT json_get_object(j, 'a.x') FROM jsons;

DROP TABLE jsons;

-- test functions with arrays --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(parse_json('["a", "bcde", "", "Long time ago, there is a little pig flying in the sky"]'), 1);

INSERT INTO jsons VALUES(parse_json('[true, false, false, false]'), 2);

INSERT INTO jsons VALUES(parse_json('[1, 0, -2147483649, 2147483648]'), 3);

INSERT INTO jsons VALUES(parse_json('[1.2, 3.1415926535897932384626, -3e123, 1e100]'), 4);

INSERT INTO jsons VALUES(parse_json('[{"a": {"i": 1}}, {"a": {"i": 2}}, {"a": {"i": 3}}]'), 5);

SELECT json_get_int(j, '[0]') FROM jsons;

SELECT json_get_float(j, '[1]') FROM jsons;

SELECT json_get_bool(j, '[2]') FROM jsons;

SELECT json_get_string(j, '[3]') FROM jsons;

SELECT json_to_string(json_get_object(j, '[0]')) FROM jsons;

SELECT json_get_int(json_get_object(j, '[0]'), 'a.i') FROM jsons;

SELECT json_get_int(json_get_object(j, '[9]'), 'a.i') FROM jsons;

SELECT json_get(j, '[0]', 'int') FROM jsons;

SELECT json_get(j, '[0]', 'float') FROM jsons;

SELECT json_get(j, '[1]', 'int') FROM jsons;

SELECT json_get(j, '[1]', 'float') FROM jsons;

SELECT json_get(j, '[2]', 'bool') FROM jsons;

SELECT json_get(j, '[3]', 'string') FROM jsons;

DROP TABLE jsons;

-- test functions in WHERE clause --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": 1}}}'), 1);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": 1.234}}}'), 2);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": "foo"}}}'), 3);

INSERT INTO jsons VALUES(parse_json('{"a": {"b": {"c": true}}}'), 4);

SELECT json_to_string(j) FROM jsons WHERE json_get_int(j, 'a.b.c') = 1;

SELECT json_to_string(j) FROM jsons WHERE json_get_float(j, 'a.b.c') = 1.234;

SELECT json_to_string(j) FROM jsons WHERE json_get_string(j, 'a.b.c') = 'foo';

SELECT json_to_string(j) FROM jsons WHERE json_get_bool(j, 'a.b.c') = true;

SELECT json_to_string(j) FROM jsons WHERE CAST(json_get_int(j, 'a.b.c') AS BOOLEAN);

SELECT json_to_string(j) FROM jsons WHERE json_get_string(json_get_object(j, 'a.b'), 'c') == 'foo';

SELECT json_to_string(j) FROM jsons WHERE json_to_string(json_get_object(j, 'a.b')) == '{"c":1}';

SELECT json_to_string(j) FROM jsons WHERE json_get_string(json_get_object(j, 'a.x'), 'c') == 'foo';

DROP TABLE jsons;
