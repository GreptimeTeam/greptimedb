create table
    skipping_table (
        ts timestamp time index,
        `id` string skipping index,
        `name` string skipping index
        with
            (granularity = 8192),
    );

show
create table
    skipping_table;

drop table skipping_table;

CREATE TABLE duplicated_skipping_index (
  `id` INT,
  host STRING,
  ts TIMESTAMP,
  PRIMARY KEY (`id`, host),
  SKIPPING INDEX (host, `id`),
  SKIPPING INDEX (host, `id`),
);

CREATE TABLE duplicated_skipping_index (
  `id` INT,
  host STRING SKIPPING INDEX,
  ts TIMESTAMP,
  PRIMARY KEY (`id`, host),
  SKIPPING INDEX (host, `id`),
);

CREATE TABLE two_skipping_indexes (
  `id` INT,
  host STRING SKIPPING INDEX,
  ts TIMESTAMP TIME INDEX,
  SKIPPING INDEX (`id`),
);

SHOW CREATE TABLE two_skipping_indexes;

DROP TABLE two_skipping_indexes;
