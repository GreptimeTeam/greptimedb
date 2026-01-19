-- Test TTL filtering at frontend during ingestion
CREATE TABLE ttl_frontend_test (
    `id` INT,
    `value` DOUBLE,
    `ts` TIMESTAMP TIME INDEX,
    PRIMARY KEY (`id`)
) WITH (
    ttl = '1h'
);

-- Try to insert data from 2 hours ago (should be filtered by frontend)
-- Using fixed old timestamp
INSERT INTO ttl_frontend_test VALUES
    (1, 1.0, '2020-01-01 10:00:00'),
    (2, 2.0, '2020-01-01 10:30:00');

-- Verify no rows were inserted (filtered at frontend)
SELECT COUNT(*) FROM ttl_frontend_test;

SELECT s.region_rows FROM information_schema.tables t JOIN information_schema.region_statistics s ON s.table_id=t.table_id  WHERE table_name='ttl_frontend_test';


-- Insert recent data using now() (should succeed)
INSERT INTO ttl_frontend_test VALUES
    (3, 3.0, now());

-- Verify only recent data exists
SELECT COUNT(*) FROM ttl_frontend_test;
SELECT s.region_rows FROM information_schema.tables t JOIN information_schema.region_statistics s ON s.table_id=t.table_id  WHERE table_name='ttl_frontend_test';

-- Test instant TTL (all data filtered immediately)
CREATE TABLE instant_ttl_test (
    `id` INT,
    `ts` TIMESTAMP TIME INDEX
) WITH (
    ttl = 'instant'
);

-- This should be filtered
INSERT INTO instant_ttl_test VALUES (1, now());

-- Verify nothing was stored
SELECT COUNT(*) FROM instant_ttl_test;

-- Cleanup
DROP TABLE ttl_frontend_test;
DROP TABLE instant_ttl_test;
