CREATE TABLE test_uddsketch (
    `id` INT PRIMARY KEY,
    `value` DOUBLE,
    `ts` timestamp time index default now()
);

INSERT INTO test_uddsketch (`id`, `value`) VALUES
    (1, 10.0),
    (2, 20.0),
    (3, 30.0),
    (4, 40.0),
    (5, 50.0),
    (6, 60.0),
    (7, 70.0),
    (8, 80.0),
    (9, 90.0),
    (10, 100.0);

select uddsketch_calc(0.1, uddsketch_state(128, 0.01, `value`)) from test_uddsketch;

select uddsketch_calc(0.5, uddsketch_state(128, 0.01, `value`)) from test_uddsketch;

select uddsketch_calc(0.75, uddsketch_state(128, 0.01, `value`)) from test_uddsketch;

select uddsketch_calc(0.95, uddsketch_state(128, 0.01, `value`)) from test_uddsketch;

SELECT
    ROUND(uddsketch_rank(5, `state`), 4) AS below_min_rank,
    ROUND(uddsketch_rank(10, `state`), 4) AS equal_min_rank,
    ROUND(uddsketch_rank(55, `state`), 4) AS middle_rank,
    ROUND(uddsketch_rank(100, `state`), 4) AS equal_max_rank,
    ROUND(uddsketch_rank(110, `state`), 4) AS above_max_rank
FROM (
    SELECT uddsketch_state(128, 0.01, `value`) AS `state`
    FROM test_uddsketch
);

CREATE TABLE grouped_uddsketch (
    `state` BINARY,
    id_group INT PRIMARY KEY,
    `ts` timestamp time index default now()
);

INSERT INTO grouped_uddsketch (`state`, id_group) SELECT uddsketch_state(128, 0.01, `value`), `id`/5*5 as id_group FROM test_uddsketch GROUP BY id_group;

SELECT uddsketch_calc(0.1, uddsketch_merge(128, 0.01, `state`)) FROM grouped_uddsketch;

SELECT ROUND(uddsketch_rank(55, uddsketch_merge(128, 0.01, `state`)), 4) AS merged_rank
FROM grouped_uddsketch;

-- should fail
SELECT uddsketch_calc(0.1, uddsketch_merge(128, 0.1, `state`)) FROM grouped_uddsketch;

-- should fail
SELECT uddsketch_calc(0.1, uddsketch_merge(64, 0.01, `state`)) FROM grouped_uddsketch;

SELECT uddsketch_rank(55, uddsketch_state(128, 0.01, `value`)) IS NULL AS empty_rank_is_null
FROM test_uddsketch
WHERE `id` < 0;

drop table test_uddsketch;

drop table grouped_uddsketch;
