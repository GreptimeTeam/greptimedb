SELECT * FROM legacy_columns ORDER BY ts;

ALTER TABLE legacy_columns MODIFY COLUMN payload COMPRESSION WITH (level = '14', page_size = '16KiB');

ALTER TABLE legacy_columns MODIFY COLUMN tokens COMPRESSION WITH (encoding = 'delta_binary_packed');

SHOW CREATE TABLE legacy_columns;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE legacy_columns;

SELECT * FROM legacy_columns ORDER BY ts;

INSERT INTO legacy_columns VALUES (3, 'after upgrade', 300);

ADMIN FLUSH_TABLE('legacy_columns');

SELECT * FROM legacy_columns ORDER BY ts;

ALTER TABLE legacy_columns MODIFY COLUMN payload UNSET COMPRESSION;

SHOW CREATE TABLE legacy_columns;

ALTER TABLE legacy_columns MODIFY COLUMN payload COMPRESSION WITH (level = '9');

SHOW CREATE TABLE legacy_columns;

INSERT INTO legacy_columns VALUES (4, 'after unset', 400);

ADMIN FLUSH_TABLE('legacy_columns');

SELECT * FROM legacy_columns ORDER BY ts;
