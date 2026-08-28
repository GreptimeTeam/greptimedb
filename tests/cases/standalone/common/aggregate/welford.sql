CREATE TABLE test_welford (
    `id` INT PRIMARY KEY,
    `value` DOUBLE,
    `ts` TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO test_welford (`id`, `value`) VALUES
    (1, 10.0),
    (2, 20.0),
    (3, 30.0),
    (4, 40.0),
    (5, 50.0),
    (6, 60.0),
    (7, 70.0),
    (8, 80.0),
    (9, 90.0),
    (10, 100.0),
    (11, NULL);

SELECT welford_stddev(welford_state(`value`)) FROM test_welford;

SELECT welford_stddev(welford_state(`value`)) FROM test_welford WHERE false;

CREATE TABLE grouped_welford (
    `id` INT PRIMARY KEY,
    `state` BINARY,
    `ts` TIMESTAMP TIME INDEX DEFAULT now()
);

INSERT INTO grouped_welford (`id`, `state`)
SELECT 1, welford_state(`value`) FROM test_welford WHERE id <= 5;

INSERT INTO grouped_welford (`id`, `state`)
SELECT 2, welford_state(`value`) FROM test_welford WHERE id > 5;

SELECT welford_stddev(welford_merge(`state`)) FROM grouped_welford;

DROP TABLE grouped_welford;
DROP TABLE test_welford;
