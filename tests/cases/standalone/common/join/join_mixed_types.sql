-- Tests joins with mixed data types and conversions

CREATE TABLE numeric_keys(int_key INTEGER, float_key DOUBLE, str_val VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE string_keys(str_key VARCHAR, int_val INTEGER, desc_val VARCHAR, ts TIMESTAMP TIME INDEX);

INSERT INTO numeric_keys VALUES (1, 1.0, 'first', 1000), (2, 2.0, 'second', 2000), (3, 3.0, 'third', 3000);

INSERT INTO string_keys VALUES ('1', 100, 'hundred', 1000), ('2', 200, 'two_hundred', 2000), ('4', 400, 'four_hundred', 3000);

SELECT n.int_key, n.str_val, s.int_val, s.desc_val FROM numeric_keys n INNER JOIN string_keys s ON CAST(n.int_key AS VARCHAR) = s.str_key ORDER BY n.int_key;

SELECT n.float_key, s.str_key, s.int_val FROM numeric_keys n LEFT JOIN string_keys s ON n.float_key = CAST(s.str_key AS DOUBLE) ORDER BY n.float_key;

DROP TABLE numeric_keys;

DROP TABLE string_keys;