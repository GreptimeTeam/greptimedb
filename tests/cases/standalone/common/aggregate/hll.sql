CREATE TABLE test_hll (
    `id` INT PRIMARY KEY,
    `value` STRING,
    `ts` timestamp time index default now()
);

INSERT INTO test_hll (`id`, `value`) VALUES
    (1, "a"),
    (2, "b"),
    (5, "e"),
    (6, "f"),
    (7, "g"),
    (8, "h"),
    (9, "i"),
    (10, "j"),
    (11, "i"),
    (12, "j"),
    (13, "i"),
    (14, "n"),
    (15, "o");

select hll_count(hll(`value`)) from test_hll;

INSERT INTO test_hll (`id`, `value`) VALUES
    (16, "b"),
    (17, "i"),
    (18, "j"),
    (19, "s"),
    (20, "t");

select hll_count(hll(`value`)) from test_hll;

create table test_hll_merge (
    `id` INT PRIMARY KEY,
    `state` BINARY,
    `ts` timestamp time index default now()
);

insert into test_hll_merge (`id`, `state`)
select 1, hll(`value`) from test_hll;

insert into test_hll_merge (`id`, `state`)
select 2, hll(`value`) from test_hll;

select hll_count(hll_merge(`state`)) from test_hll_merge;

CREATE TABLE test_hll_delta_merge (
    `id` INT PRIMARY KEY,
    `delta` BINARY,
    `persisted` BINARY,
    `ts` timestamp time index default now()
);

INSERT INTO test_hll_delta_merge (`id`, `delta`, `persisted`)
SELECT 1, hll(`value`), NULL FROM test_hll WHERE `id` <= 15;

INSERT INTO test_hll_delta_merge (`id`, `delta`, `persisted`)
SELECT 2, NULL, hll(`value`) FROM test_hll WHERE `id` > 15;

SELECT
    hll_count(__hll_delta_merge(`delta`, `persisted`)) AS merged_count,
    (SELECT hll_count(hll(`value`)) FROM test_hll) AS direct_count
FROM test_hll_delta_merge;

drop table test_hll;

drop table test_hll_merge;

drop table test_hll_delta_merge;
