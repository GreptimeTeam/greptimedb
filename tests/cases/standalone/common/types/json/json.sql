CREATE TABLE jsons (j JSON, t timestamp time index);

--Insert valid json strings--
INSERT INTO jsons VALUES('{"a":1, "b":2, "c":3}', 1), ('{"a":1, "b":2, "c":3, "d":4}', 2), ('{"a":1, "b":2, "c":3, "d":4, "e":5}', 3);

SELECT * FROM jsons;

--Insert invalid hex strings--
DELETE FROM jsons;

INSERT INTO jsons VALUES('\xaa\xff\xaa'::BYTEA, 1), ('\xaa\xff\xaa\xaa\xff\xaa'::BYTEA, 2), ('\xaa\xff\xaa\xaa\xff\xaa\xaa\xff\xaa'::BYTEA, 3);

SELECT * FROM blobs;

--Insert invalid json strings--
DELETE FROM jsons;

INSERT INTO jsons VALUES('{"a":1, "b":2, "c":3', 4);

INSERT INTO jsons VALUES('Morning my friends, have a nice day :)', 5);

SELECT * FROM jsons;

CREATE TABLE json_empty (j JSON, t timestamp time index);

INSERT INTO json_empty VALUES(NULL, 2);

SELECT * FROM json_empty;

drop table jsons;

drop table json_empty;
