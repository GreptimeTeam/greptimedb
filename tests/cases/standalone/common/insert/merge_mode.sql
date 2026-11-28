create table if not exists last_non_null_table(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_non_null');

INSERT INTO last_non_null_table VALUES ('host1', 0, 0, NULL), ('host2', 1, NULL, 1);

INSERT INTO last_non_null_table VALUES ('host1', 0, NULL, 10), ('host2', 1, 11, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

INSERT INTO last_non_null_table VALUES ('host1', 0, 20, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

INSERT INTO last_non_null_table VALUES ('host1', 0, NULL, NULL);

SELECT * from last_non_null_table ORDER BY host, ts;

DROP TABLE last_non_null_table;

create table if not exists last_row_table(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_row');

INSERT INTO last_row_table VALUES ('host1', 0, 0, NULL), ('host2', 1, NULL, 1);

INSERT INTO last_row_table VALUES ('host1', 0, NULL, 10), ('host2', 1, 11, NULL);

SELECT * from last_row_table ORDER BY host, ts;

DROP TABLE last_row_table;

CREATE TABLE IF NOT EXISTS `delete_between` (
  `time` TIMESTAMP(0) NOT NULL,
  `code` STRING NULL,
  `name` STRING NULL,
  `status` TINYINT NULL,
  TIME INDEX (`time`),
  PRIMARY KEY (`code`)
) ENGINE=mito WITH(
  merge_mode = 'last_non_null'
);

INSERT INTO `delete_between` (`time`, `code`, `name`, `status`) VALUES ('2024-11-26 10:00:00', 'achn', '1.png', 0);
INSERT INTO `delete_between` (`time`, `code`, `name`, `status`) VALUES ('2024-11-26 10:01:00', 'achn', '2.png', 0);
INSERT INTO `delete_between` (`time`, `code`, `name`, `status`) VALUES ('2024-11-26 10:02:00', 'achn', '3.png', 1);

SELECT * FROM `delete_between`;

DELETE FROM `delete_between`;

INSERT INTO `delete_between` (`time`, `code`, `name`) VALUES ('2024-11-26 10:00:00', 'achn', '1.png');
INSERT INTO `delete_between` (`time`, `code`, `name`) VALUES ('2024-11-26 10:01:00', 'achn', '2.png');
INSERT INTO `delete_between` (`time`, `code`, `name`) VALUES ('2024-11-26 10:02:00', 'achn', '3.png');

SELECT * FROM `delete_between`;

DROP TABLE `delete_between`;

CREATE TABLE IF NOT EXISTS `delete_reinsert` (
  `host` STRING,
  `ts` TIMESTAMP TIME INDEX,
  `cpu` DOUBLE NULL,
  `mem` BIGINT NULL,
  PRIMARY KEY (`host`)
) ENGINE=mito WITH(
  merge_mode = 'last_non_null'
);

INSERT INTO `delete_reinsert` VALUES ('h2', 2000, 1.0, NULL);

ADMIN FLUSH_TABLE('delete_reinsert');

INSERT INTO `delete_reinsert` VALUES ('h1', 1000, 1.0, NULL);

INSERT INTO `delete_reinsert` VALUES ('h1', 1000, NULL, 20);

DELETE FROM `delete_reinsert` WHERE `host` = 'h1' AND `ts` = 1000;

INSERT INTO `delete_reinsert` VALUES ('h1', 2000, NULL, NULL);

INSERT INTO `delete_reinsert` VALUES ('h1', 1000, NULL, 10);

-- Values written before the delete must not fill the NULL fields.
SELECT * FROM `delete_reinsert` ORDER BY `host`, `ts`;

DROP TABLE `delete_reinsert`;

-- SQLNESS REPLACE (\sat\sline\s\d+\scolumn\s\d+)
create table if not exists invalid_merge_mode(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='first_row');

create table if not exists invalid_merge_mode(
    host string,
    ts timestamp,
    cpu double,
    memory double,
    TIME INDEX (ts),
    PRIMARY KEY(host)
)
engine=mito
with('merge_mode'='last_non_null', 'append_mode'='true');
