-- Migrated from DuckDB test: test/sql/window/test_lead_lag.test

CREATE TABLE win("id" INTEGER, v INTEGER, t INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO win VALUES
(1, 1, 2, 1000),
(1, 1, 1, 2000), 
(1, 2, 3, 3000),
(2, 10, 4, 4000),
(2, 11, -1, 5000);

-- LAG function with offset 2
SELECT "id", v, t, LAG(v, 2, NULL) OVER (PARTITION BY "id" ORDER BY t ASC) as lag_val
FROM win ORDER BY "id", t;

-- LEAD function with offset 1
SELECT "id", v, t, LEAD(v, 1, -999) OVER (PARTITION BY "id" ORDER BY t ASC) as lead_val  
FROM win ORDER BY "id", t;

-- LAG with default value
SELECT v, LAG(v, 1, 0) OVER (ORDER BY t) as lag_with_default FROM win ORDER BY t;

-- LEAD with offset 2 and default value
SELECT "id", v, t, LEAD(v, 2, -999) OVER (PARTITION BY "id" ORDER BY t ASC) as lead_val2
FROM win ORDER BY "id", t;

DROP TABLE win;

-- Test with VALUES clause (similar to DuckDB original)
SELECT c1, LEAD(c1, 2) OVER (ORDER BY c0) as lead_val
FROM (VALUES 
    (1, 2, 1000), 
    (2, 3, 2000), 
    (3, 4, 3000), 
    (4, 5, 4000)
) a(c0, c1, ts) ORDER BY c0;