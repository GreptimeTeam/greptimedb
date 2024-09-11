-- json_get functions --
SELECT json_get_int(to_json('{"a": {"b": {"c": 1}}}'), 'a.b.c');

SELECT json_get_float(to_json('{"a": {"b": {"c": 1.234}}}'), 'a:b.c');

SELECT json_get_string(to_json('{"a": {"b": {"c": "foo"}}}'), 'a.b:c');

SELECT json_get_bool(to_json('{"a": {"b": {"c": true}}}'), 'a.b["c"]');

SELECT json_get_int(to_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

SELECT json_get_string(to_json('{"a": {"b": {"c": {"d": 1}}}}'), 'a.b');

-- test functions with table rows --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": 1}}}'), 1);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": 1.234}}}'), 2);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": "foo"}}}'), 3);

INSERT INTO jsons VALUES(to_json('{"a": {"b": {"c": true}}}'), 4);

SELECT json_get_int(j, 'a.b.c') FROM jsons;

SELECT json_get_float(j, 'a["b"].c') FROM jsons;

SELECT json_get_string(j, 'a.b.c?(@ == 1)') FROM jsons;

SELECT json_get_bool(j, 'a.b.c') FROM jsons;

SELECT json_get_int(j, 'a.b["c"]') FROM jsons;

DROP TABLE jsons;

-- test functions with arrays --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(to_json('["a", "bcde", "", "Long time ago, there is a little pig flying in the sky"]'), 1);

INSERT INTO jsons VALUES(to_json('[true, false, false, false]'), 2);

INSERT INTO jsons VALUES(to_json('[1, 0, -2147483649, 2147483648]'), 3);

INSERT INTO jsons VALUES(to_json('[1.2, 3.1415926535897932384626, -3e123, 1e100]'), 4);

SELECT json_get_int(j, '[0]') FROM jsons;

SELECT json_get_float(j, '[1]') FROM jsons;

SELECT json_get_bool(j, '[2]') FROM jsons;

SELECT json_get_string(j, '[3]') FROM jsons;

DROP TABLE jsons;
