-- json_is functions --
SELECT json_is_object(parse_json('{"a": 1}'));

SELECT json_is_array(parse_json('[1, 2, 3]'));

SELECT json_is_int(parse_json('1'));

SELECT json_is_bool(parse_json('true'));

SELECT json_is_null(parse_json('null'));

SELECT json_is_float(parse_json('1.2'));

SELECT json_is_string(parse_json('"foo"'));

SELECT json_is_null(parse_json('{"a": 1}'));

SELECT json_is_string(parse_json('[1, 2, 3]'));

SELECT json_is_float(parse_json('1'));

-- test json_is functions in table rows and WHERE clause --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(parse_json('{"a": 1}'), 1);

INSERT INTO jsons VALUES(parse_json('[1, 2, 3]'), 2);

INSERT INTO jsons VALUES(parse_json('1'), 3);

INSERT INTO jsons VALUES(parse_json('true'), 4);

INSERT INTO jsons VALUES(parse_json('null'), 5);

INSERT INTO jsons VALUES(parse_json('1.2'), 6);

INSERT INTO jsons VALUES(parse_json('"foo"'), 7);

SELECT json_to_string(j) FROM jsons WHERE json_is_object(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_array(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_int(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_bool(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_null(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_float(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_string(j);

DROP TABLE jsons;
