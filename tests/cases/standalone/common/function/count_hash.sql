create table
    t (
        ts timestamp time index,
        `id` int,
        `name` string,
        PRIMARY KEY (`id`),
    );

insert into
    t (ts, `id`, `name`)
values
    (1, 1, 'a'),
    (2, 2, 'b'),
    (3, 3, 'c'),
    (4, 4, 'd');

select
    count_hash (`id`)
from
    t;

select
    count_hash (`id`)
from
    t
group by
    `name`;

select
    count_hash (`id`, `name`)
from
    t
group by
    ts;

drop table t;
