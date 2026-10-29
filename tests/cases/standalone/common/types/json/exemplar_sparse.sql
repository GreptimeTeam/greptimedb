-- A shared table's TSID does not imply ordering across source table IDs.
CREATE TABLE exemplar_tsid_order (
    __table_id INT UNSIGNED,
    __tsid BIGINT UNSIGNED,
    greptime_metric STRING,
    ts TIMESTAMP TIME INDEX,
    greptime_value DOUBLE,
    PRIMARY KEY (__table_id, __tsid, greptime_metric)
) WITH ('append_mode'='true');
INSERT INTO exemplar_tsid_order VALUES
    (1, 7, 'cpu', 20, 1), (2, 7, 'memory', 10, 2);
SELECT __tsid, ts, greptime_value FROM exemplar_tsid_order ORDER BY __tsid, ts;
ADMIN FLUSH_TABLE('exemplar_tsid_order');
SELECT __tsid, ts, greptime_value FROM exemplar_tsid_order ORDER BY __tsid, ts;
DROP TABLE exemplar_tsid_order;
