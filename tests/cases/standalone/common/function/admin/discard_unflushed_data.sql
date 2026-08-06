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
