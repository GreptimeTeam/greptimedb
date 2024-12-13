create table
    skipping_table (
        ts timestamp time index,
        id string skip,
        `name` string skip
        with
            (granularity = 8192),
    );

show
create table
    skipping_table;

drop table skipping_table;