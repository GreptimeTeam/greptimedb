create table
    skipping_table (
        ts timestamp time index,
        `id` string skipping index,
        `name` string skipping index
        with
            (
                granularity = 8192,
                false_positive_rate = 0.05,
                type = 'BLOOM',
            ),
    );

show
create table
    skipping_table;

drop table skipping_table;
