-- Migrated from DuckDB test style: test array aggregation
-- Test ARRAY_AGG function

-- Test with integers
CREATE TABLE integers(i INTEGER, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO integers VALUES (1, 1, 1000), (2, 1, 2000), (3, 1, 3000), (4, 2, 4000), (5, 2, 5000);

-- Basic array aggregation
SELECT array_agg(i) FROM integers;

-- Array aggregation with GROUP BY
SELECT g, array_agg(i) FROM integers GROUP BY g ORDER BY g;

-- Test with ORDER BY
SELECT array_agg(i ORDER BY i DESC) FROM integers;

SELECT g, array_agg(i ORDER BY i DESC) FROM integers GROUP BY g ORDER BY g;

-- Test with strings
CREATE TABLE strings(s VARCHAR, g INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO strings VALUES 
    ('apple', 1, 1000), ('banana', 1, 2000), ('cherry', 2, 3000), 
    ('date', 2, 4000), ('elderberry', 1, 5000);

SELECT array_agg(s) FROM strings;

SELECT g, array_agg(s ORDER BY s) FROM strings GROUP BY g ORDER BY g;

-- Test with NULL values
INSERT INTO strings VALUES (NULL, 1, 6000), ('fig', NULL, 7000);

SELECT array_agg(s) FROM strings WHERE s IS NOT NULL;

SELECT g, array_agg(s) FROM strings WHERE g IS NOT NULL GROUP BY g ORDER BY g;

-- Test with DISTINCT
SELECT array_agg(DISTINCT s ORDER BY s) FROM strings WHERE s IS NOT NULL;

-- Test empty result
SELECT array_agg(i) FROM integers WHERE i > 100;

-- Test with doubles
CREATE TABLE doubles(d DOUBLE, ts TIMESTAMP TIME INDEX);

INSERT INTO doubles VALUES (1.1, 1000), (2.2, 2000), (3.3, 3000), (4.4, 4000);

SELECT array_agg(d ORDER BY d) FROM doubles;

-- cleanup
DROP TABLE integers;

DROP TABLE strings;

DROP TABLE doubles;