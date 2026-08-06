CREATE TABLE discard_unflushed_data_test (
    host STRING PRIMARY KEY,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
) ENGINE = mito;

INSERT INTO discard_unflushed_data_test VALUES ('persisted', 1, 1);

ADMIN FLUSH_TABLE('discard_unflushed_data_test');

INSERT INTO discard_unflushed_data_test VALUES ('unflushed', 2, 2);

ADMIN discard_unflushed_data('discard_unflushed_data_test');

-- Repeating the operation is idempotent.
ADMIN discard_unflushed_data('discard_unflushed_data_test');

SELECT host, val FROM discard_unflushed_data_test ORDER BY host;

-- The function must only be available through ADMIN.
SELECT discard_unflushed_data(0);

DROP TABLE discard_unflushed_data_test;

CREATE TABLE discard_unflushed_data_physical (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE discard_unflushed_data_logical (
    host STRING PRIMARY KEY,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
) ENGINE = metric WITH ("on_physical_table" = "discard_unflushed_data_physical");

INSERT INTO discard_unflushed_data_logical VALUES ('unflushed', 1, 1);

-- Logical Metric Engine tables share their physical regions with other logical tables.
-- Discarding by logical table name must not truncate those shared regions.
ADMIN discard_unflushed_data('discard_unflushed_data_logical');

SELECT host, val FROM discard_unflushed_data_logical;

DROP TABLE discard_unflushed_data_logical;

DROP TABLE discard_unflushed_data_physical;
