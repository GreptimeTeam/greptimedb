CREATE TABLE jsons (j JSON, t timestamp time index);

--Insert valid json strings--
INSERT INTO jsons VALUES(to_json('{"a":1, "b":2, "c":3}'), 1), (to_json('{"a":1, "b":2, "c":3, "d":4}'), 2), (to_json('{"a":1, "b":2, "c":3, "d":4, "e":5}'), 3);

SELECT to_string(j), t FROM jsons;

--Insert invalid json strings--
DELETE FROM jsons;

INSERT INTO jsons VALUES(to_json('{"a":1, "b":2, "c":3'), 4);

INSERT INTO jsons VALUES(to_json('Morning my friends, have a nice day :)'), 5);

SELECT to_string(j), t FROM jsons;

CREATE TABLE json_empty (j JSON, t timestamp time index);

INSERT INTO json_empty VALUES(NULL, 2);

SELECT to_json(j), t FROM json_empty;

drop table jsons;

drop table json_empty;
