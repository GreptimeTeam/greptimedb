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

select hll_calc(hll_state(`value`)) from test_hll;

INSERT INTO test_hll (`id`, `value`) VALUES
    (16, "b"),
    (17, "i"),
    (18, "j"),
    (19, "s"),
    (20, "t");

select hll_calc(hll_state(`value`)) from test_hll;

drop table test_hll; 
