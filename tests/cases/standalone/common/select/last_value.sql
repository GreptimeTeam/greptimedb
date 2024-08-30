create table t (
    ts timestamp time index,
    host string primary key,
    not_pk string,
    val double,
) with (append_mode='true');

insert into t values
    (0, 'a', '🌕', 1.0),
    (1, 'b', '🌖', 2.0),
    (2, 'a', '🌗', 3.0),
    (3, 'c', '🌘', 4.0),
    (4, 'a', '🌑', 5.0),
    (5, 'b', '🌒', 6.0),
    (6, 'a', '🌓', 7.0),
    (7, 'c', '🌔', 8.0),
    (8, 'd', '🌕', 9.0);

admin flush_table('t');

select
        last_value(host order by ts),
        last_value(not_pk order by ts),
        last_value(val order by ts)
from t
    group by host
    order by host;

-- repeat the query again, ref: https://github.com/GreptimeTeam/greptimedb/issues/4650
select
        last_value(host order by ts),
        last_value(not_pk order by ts),
        last_value(val order by ts)
from t
    group by host
    order by host;

drop table t;
