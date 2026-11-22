-- COPY FROM keeps every digit of a DECIMAL or BIGINT UNSIGNED value from a csv or json file.
CREATE TABLE exact_decimals(k STRING, price DECIMAL(38, 10), amount DECIMAL(20, 2), ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

INSERT INTO exact_decimals VALUES
    ('a', CAST('213865087984.2783187654' AS DECIMAL(38, 10)), CAST('123456789012345678.12' AS DECIMAL(20, 2)), 1000),
    ('b', CAST('0.1234567891' AS DECIMAL(38, 10)), CAST('-0.01' AS DECIMAL(20, 2)), 2000),
    ('c', CAST('-9999999999999999999999999999.9999999999' AS DECIMAL(38, 10)), CAST('7' AS DECIMAL(20, 2)), 3000),
    ('d', NULL, NULL, 4000);

Copy exact_decimals TO '${SQLNESS_HOME}/demo/export/exact_numbers/decimals.csv' WITH (format='csv');

CREATE TABLE exact_decimals_csv(k STRING, price DECIMAL(38, 10), amount DECIMAL(20, 2), ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

Copy exact_decimals_csv FROM '${SQLNESS_HOME}/demo/export/exact_numbers/decimals.csv' WITH (format='csv');

SELECT k, CAST(price AS STRING) AS price, CAST(amount AS STRING) AS amount FROM exact_decimals_csv ORDER BY k;

Copy exact_decimals TO '${SQLNESS_HOME}/demo/export/exact_numbers/decimals.json' WITH (format='json');

CREATE TABLE exact_decimals_json(k STRING, price DECIMAL(38, 10), amount DECIMAL(20, 2), ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

Copy exact_decimals_json FROM '${SQLNESS_HOME}/demo/export/exact_numbers/decimals.json' WITH (format='json');

SELECT k, CAST(price AS STRING) AS price, CAST(amount AS STRING) AS amount FROM exact_decimals_json ORDER BY k;

CREATE TABLE exact_integers(k STRING, u BIGINT UNSIGNED, i BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

INSERT INTO exact_integers VALUES
    ('a', 9007199254740993, 9007199254740993, 1000),
    ('b', 18446744073709551615, 9223372036854775807, 2000),
    ('c', 1955706447374193284, 1955706447374193284, 3000),
    ('d', 0, -9223372036854775808, 4000),
    ('e', NULL, NULL, 5000);

Copy exact_integers TO '${SQLNESS_HOME}/demo/export/exact_numbers/integers.json' WITH (format='json');

CREATE TABLE exact_integers_json(k STRING, u BIGINT UNSIGNED, i BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

Copy exact_integers_json FROM '${SQLNESS_HOME}/demo/export/exact_numbers/integers.json' WITH (format='json');

SELECT k, CAST(u AS STRING) AS u, CAST(i AS STRING) AS i FROM exact_integers_json ORDER BY k;

-- The value above the BIGINT range comes after the rows that schema inference reads.
CREATE TABLE exact_integers_late_json(k STRING, u BIGINT UNSIGNED, i BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

Copy exact_integers_late_json FROM '${SQLNESS_HOME}/demo/export/exact_numbers/integers.json' WITH (format='json', schema_infer_max_record=1);

SELECT k, CAST(u AS STRING) AS u, CAST(i AS STRING) AS i FROM exact_integers_late_json ORDER BY k;

Copy exact_integers TO '${SQLNESS_HOME}/demo/export/exact_numbers/integers.csv' WITH (format='csv');

CREATE TABLE exact_integers_csv(k STRING, u BIGINT UNSIGNED, i BIGINT, ts TIMESTAMP TIME INDEX, PRIMARY KEY(k));

Copy exact_integers_csv FROM '${SQLNESS_HOME}/demo/export/exact_numbers/integers.csv' WITH (format='csv');

SELECT k, CAST(u AS STRING) AS u, CAST(i AS STRING) AS i FROM exact_integers_csv ORDER BY k;

DROP TABLE exact_decimals;

DROP TABLE exact_decimals_csv;

DROP TABLE exact_decimals_json;

DROP TABLE exact_integers;

DROP TABLE exact_integers_csv;

DROP TABLE exact_integers_json;

DROP TABLE exact_integers_late_json;
