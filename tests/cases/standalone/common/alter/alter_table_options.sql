CREATE TABLE ato(i INTEGER, j TIMESTAMP TIME INDEX, PRIMARY KEY(i));

INSERT INTO ato VALUES(1, now()), (2, now());

SELECT i FROM ato;

ALTER TABLE ato SET 'ttl'='1d';

SELECT i FROM ato;

SHOW CREATE TABLE ato;

ALTER TABLE ato SET 'ttl'='2d';

SELECT i FROM ato;

SHOW CREATE TABLE ato;

ALTER TABLE ato SET 'ttl'=NULL;

SELECT i FROM ato;

SHOW CREATE TABLE ato;

ALTER TABLE ato SET 'ttl'='1s';

SHOW CREATE TABLE ato;

ALTER TABLE ato SET 'ttl'='😁';

ALTER TABLE ato SET '🕶️'='1s';

SELECT i FROM ato;

ALTER TABLE ato SET 'compaction.twcs.time_window'='2h';

ALTER TABLE ato SET 'compaction.twcs.max_output_file_size'='500MB';

ALTER TABLE ato SET 'compaction.twcs.trigger_file_num'='2';

SHOW CREATE TABLE ato;

ALTER TABLE ato UNSET 'compaction.twcs.time_window';

ALTER TABLE ato UNSET '🕶️';

SHOW CREATE TABLE ato;

ALTER TABLE ato SET 'compaction.twcs.trigger_file_num'='';

SHOW CREATE TABLE ato;

-- SQLNESS ARG restart=true
SHOW CREATE TABLE ato;

DROP TABLE ato;

CREATE TABLE phy (ts timestamp time index, val double) engine=metric with ("physical_metric_table" = "");

ALTER TABLE phy set ttl='2years';

SHOW CREATE TABLE phy;

ALTER TABLE phy UNSET 'ttl';

SHOW CREATE TABLE phy;

DROP TABLE phy;
