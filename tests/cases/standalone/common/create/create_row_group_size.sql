CREATE TABLE row_group_tbl (
    host STRING,
    cpu DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host)
) WITH ('max_row_group_row_count' = '1024');

-- The option should be persisted and shown.
SHOW CREATE TABLE row_group_tbl;

INSERT INTO row_group_tbl VALUES
    ('host1', 1.0, 1667446797450),
    ('host2', 2.0, 1667446797450);

ADMIN flush_table('row_group_tbl');

-- SQLNESS SORT_RESULT 3 1
SELECT * FROM row_group_tbl;

ALTER TABLE row_group_tbl SET 'max_row_group_row_count' = '2048';

SHOW CREATE TABLE row_group_tbl;

-- Invalid altered values are rejected.
ALTER TABLE row_group_tbl SET 'max_row_group_row_count' = '0';

-- UNSET restores the engine default and removes the option from table metadata.
ALTER TABLE row_group_tbl UNSET 'max_row_group_row_count';

SHOW CREATE TABLE row_group_tbl;

DROP TABLE row_group_tbl;

-- A zero value is rejected.
CREATE TABLE row_group_bad (
    host STRING,
    cpu DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY(host)
) WITH ('max_row_group_row_count' = '0');
