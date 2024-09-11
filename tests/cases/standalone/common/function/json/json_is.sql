-- json_is functions --
SELECT json_is_object(to_json('{"a": 1}'));

SELECT json_is_array(to_json('[1, 2, 3]'));

SELECT json_is_int(to_json('1'));

SELECT json_is_bool(to_json('true'));

SELECT json_is_null(to_json('null'));

SELECT json_is_float(to_json('1.2'));

SELECT json_is_string(to_json('"foo"'));

SELECT json_is_null(to_json('{"a": 1}'));

SELECT json_is_string(to_json('[1, 2, 3]'));

SELECT json_is_float(to_json('1'));

-- test json_is functions in table rows and WHERE clause --
CREATE TABLE jsons(j JSON, ts timestamp time index);

INSERT INTO jsons VALUES(to_json('{"a": 1}'), 1);

INSERT INTO jsons VALUES(to_json('[1, 2, 3]'), 2);

INSERT INTO jsons VALUES(to_json('1'), 3);

INSERT INTO jsons VALUES(to_json('true'), 4);

INSERT INTO jsons VALUES(to_json('null'), 5);

INSERT INTO jsons VALUES(to_json('1.2'), 6);

INSERT INTO jsons VALUES(to_json('"foo"'), 7);

SELECT json_to_string(j) FROM jsons WHERE json_is_object(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_array(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_int(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_bool(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_null(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_float(j);

SELECT json_to_string(j) FROM jsons WHERE json_is_string(j);

DROP TABLE jsons;
