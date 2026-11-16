CREATE TABLE discard_pending_tails_test (
    host STRING PRIMARY KEY,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
) PARTITION ON COLUMNS (host) (
    host < 'm',
    host >= 'm'
) ENGINE = mito;

INSERT INTO discard_pending_tails_test VALUES ('kept', 1, 1), ('remains', 2, 2);

-- No region of the table has pending tails from the node, so nothing is discarded.
ADMIN discard_pending_tails('discard_pending_tails_test', 1);

SELECT host, val FROM discard_pending_tails_test ORDER BY host;

-- The function must only be available through ADMIN.
-- SQLNESS REPLACE \nDid\syou\smean.*
SELECT discard_pending_tails('discard_pending_tails_test', 1);

DROP TABLE discard_pending_tails_test;

CREATE TABLE discard_pending_tails_physical (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE
) ENGINE = metric WITH ("physical_metric_table" = "");

CREATE TABLE discard_pending_tails_logical (
    host STRING PRIMARY KEY,
    val DOUBLE,
    ts TIMESTAMP TIME INDEX
) ENGINE = metric WITH ("on_physical_table" = "discard_pending_tails_physical");

ADMIN discard_pending_tails('discard_pending_tails_physical', 1);

-- Logical Metric Engine tables share their physical regions with other logical tables.
ADMIN discard_pending_tails('discard_pending_tails_logical', 1);

DROP TABLE discard_pending_tails_logical;

DROP TABLE discard_pending_tails_physical;

-- A region id reaches the region path; this one belongs to no table.
ADMIN discard_pending_tails(4398046511104, 1);
