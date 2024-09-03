create table
    t (
        ts timestamp time index,
        host string primary key,
        not_pk string,
        val double,
    )
with
    (
        append_mode = 'true',
        'compaction.type' = 'twcs',
        'compaction.twcs.max_active_window_files' = '8',
        'compaction.twcs.max_inactive_window_files' = '8'
    );

insert into
    t
values
    (0, 'a', '🌕', 1.0),
    (1, 'b', '🌖', 2.0),
    (1, 'a', '🌗', 3.0),
    (1, 'c', '🌘', 4.0),
    (2, 'a', '🌑', 5.0),
    (2, 'b', '🌒', 6.0),
    (2, 'a', '🌓', 7.0),
    (3, 'c', '🌔', 8.0),
    (3, 'd', '🌕', 9.0);

admin flush_table ('t');

insert into
    t
values
    (10, 'a', '🌕', 1.0),
    (11, 'b', '🌖', 2.0),
    (11, 'a', '🌗', 3.0),
    (11, 'c', '🌘', 4.0),
    (12, 'a', '🌑', 5.0),
    (12, 'b', '🌒', 6.0),
    (12, 'a', '🌓', 7.0),
    (13, 'c', '🌔', 8.0),
    (13, 'd', '🌕', 9.0);

admin flush_table ('t');

select
    count(ts)
from
    t;

select
    ts
from
    t
order by
    ts;

drop table t;
