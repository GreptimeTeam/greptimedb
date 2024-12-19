create table
    skipping_table (
        ts timestamp time index,
        id string skipping index,
        `name` string skipping index
        with
            (granularity = 8192),
    );

show
create table
    skipping_table;

drop table skipping_table;
