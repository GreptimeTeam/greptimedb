CREATE TABLE exemplar_sparse (
    greptime_timestamp TIMESTAMP(9) TIME INDEX,
    greptime_metric STRING,
    job STRING,
    greptime_value DOUBLE,
    exemplar_labels JSON2,
    PRIMARY KEY (greptime_metric, job)
) WITH ('append_mode'='true', 'primary_key_encoding'='sparse', 'sst_format'='primary_key');

INSERT INTO exemplar_sparse VALUES
    (1720000000123456789, 'latency', 'api', 0.32, '{"trace_id":"abc"}'),
    (1720000000123456789, 'latency', 'api', 0.32, '{"trace_id":"abc"}'),
    (-1, NULL, NULL, 1.0, '{}');
ADMIN FLUSH_TABLE('exemplar_sparse');
ALTER TABLE exemplar_sparse ADD COLUMN zone STRING PRIMARY KEY;
INSERT INTO exemplar_sparse (greptime_timestamp, greptime_metric, greptime_value, exemplar_labels, zone)
VALUES (1720000000123456790, 'count', 2.0, '{"span_id":"def"}', 'east');
ADMIN FLUSH_TABLE('exemplar_sparse');
ADMIN COMPACT_TABLE('exemplar_sparse', 'swcs', '86400');
SELECT greptime_timestamp, greptime_metric, job, zone, greptime_value, exemplar_labels
FROM exemplar_sparse ORDER BY greptime_timestamp, greptime_metric;
SELECT COUNT(*) FROM exemplar_sparse WHERE greptime_metric = 'latency' AND job = 'api';
SELECT COUNT(*) FROM exemplar_sparse WHERE greptime_metric IS NULL AND job IS NULL AND zone IS NULL;
SELECT COUNT(*) FROM exemplar_sparse WHERE zone = 'east' AND exemplar_labels.span_id::STRING = 'def';
DROP TABLE exemplar_sparse;

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
