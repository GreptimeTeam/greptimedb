-- Regression for offset direction: positive offsets should query past data.

create table offset_direction (
    ts timestamp time index,
    val double,
    host string primary key
);

insert into offset_direction values
    (940000, 10.0, 'a'),
    (1000000, 20.0, 'a'),
    (1060000, 30.0, 'a');

tql eval (1000, 1000, '1s') offset_direction;

tql eval (1000, 1000, '1s') offset_direction offset 60s;

tql eval (1000, 1000, '1s') offset_direction offset -60s;

drop table offset_direction;
