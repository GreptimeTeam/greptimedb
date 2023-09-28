create table completion(
    ts timestamp time index,
    model string primary key,
    val double
);

insert into completion values
    (0, 'model-a', 10),
    (5000, 'model-b', 20),
    (10000, 'model-a', 30);

create table prompt(
    ts timestamp time index,
    model string primary key,
    val double
);

insert into prompt values
    (0, 'model-a', 100),
    (5000, 'model-b', 200),
    (10000, 'model-a', 300);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 10, '5s') sum(completion * 0.0015 / 1000) + sum(prompt / 1000 * 0.0015);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 10, '5s') sum(completion * 0.0015 / 1000) + sum(prompt * 0.0015 / 1000);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 10, '5s') sum(completion * 0.0015 / 1000) by (model) + sum(prompt * 0.0015 / 1000) by (model);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 10, '5s') sum(completion / 1000) + max(completion / 1000);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 10, '5s') sum(completion / 1000) + sum(completion / 1000);

drop table completion;

drop table prompt;
