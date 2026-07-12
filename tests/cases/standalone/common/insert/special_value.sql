create table data (ts timestamp(3) time index, val double);

insert into data values
    (0, 'infinity'::double),
    (1, '-infinity'::double),
    (2, 'nan'::double),
    (3, 'NaN'::double);

select * from data;

insert into data values (4, 'infinityyyy'::double);

drop table data;
