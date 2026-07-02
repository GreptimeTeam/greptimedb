-- Document the current aliased SQL LATERAL limitation and guard the remote
-- scan boundary. DataFusion's DecorrelateLateralJoin does not currently match
-- the SubqueryAlias(Subquery) shape produced by `LATERAL (...) d`, so this query
-- is still expected to fail physical planning with an outer_ref expression. The
-- important regression assertion is that the remaining outer_ref predicate must
-- NOT be advertised as a remote TableScan.partial_filters predicate.

CREATE TABLE lateral_fact (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    val DOUBLE,
    PRIMARY KEY (k)
) ENGINE = mito;

CREATE TABLE lateral_dim (
    ts TIMESTAMP(3) TIME INDEX,
    k STRING,
    threshold DOUBLE,
    PRIMARY KEY (k)
) ENGINE = mito;

INSERT INTO lateral_fact VALUES
    ('2024-01-30 00:00:00', 'a', 10.0),
    ('2024-01-30 01:00:00', 'b', 20.0);

INSERT INTO lateral_dim VALUES
    ('2024-01-30 00:00:00', 'a', 5.0),
    ('2024-01-30 01:00:00', 'b', 25.0);

ADMIN FLUSH_TABLE('lateral_fact');
ADMIN FLUSH_TABLE('lateral_dim');

-- SQLNESS REPLACE region=\d+\(\d+,\s+\d+\) region=REDACTED
-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN SELECT f.k, d.threshold
FROM lateral_fact f,
LATERAL (
    SELECT threshold FROM lateral_dim d WHERE d.k = f.k
) d
WHERE f.val > d.threshold
ORDER BY f.k;

DROP TABLE lateral_fact;
DROP TABLE lateral_dim;
