CREATE TABLE phy (ts timestamp time index, val double, a_label STRING, PRIMARY KEY(a_label)) engine=metric with ("physical_metric_table" = "");

ALTER TABLE phy ADD COLUMN b_label STRING PRIMARY KEY;

ALTER TABLE phy DROP COLUMN a_label;

ALTER TABLE phy SET 'ttl'='1d';

SHOW CREATE TABLE phy;

ALTER TABLE phy UNSET 'ttl';

SHOW CREATE TABLE phy;

ALTER TABLE phy MODIFY COLUMN a_label SET INVERTED INDEX;

SHOW CREATE TABLE phy;

ALTER TABLE phy MODIFY COLUMN a_label UNSET INVERTED INDEX;

SHOW CREATE TABLE phy;

DROP TABLE phy;
