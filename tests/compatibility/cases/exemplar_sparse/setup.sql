CREATE TABLE metrics_exemplars (
    greptime_timestamp TIMESTAMP(9) TIME INDEX,
    greptime_metric STRING,
    job STRING,
    greptime_value DOUBLE,
    exemplar_labels JSON2,
    PRIMARY KEY (greptime_metric, job)
) WITH ('append_mode'='true', 'primary_key_encoding'='sparse', 'sst_format'='primary_key');
INSERT INTO metrics_exemplars VALUES
    (1720000000123456789, 'latency', 'api', 0.32, '{"trace_id":"abc"}'),
    (1720000000123456789, 'latency', 'api', 0.32, '{"trace_id":"abc"}'),
    (-1, NULL, NULL, 1.0, '{}');
ADMIN FLUSH_TABLE('metrics_exemplars');
ALTER TABLE metrics_exemplars ADD COLUMN zone STRING PRIMARY KEY;
INSERT INTO metrics_exemplars (greptime_timestamp, greptime_metric, greptime_value, exemplar_labels, zone)
VALUES (1720000000123456790, 'count', 2.0, '{"span_id":"def"}', 'east');

CREATE TABLE exemplar_source_ids (
    __table_id INT UNSIGNED NOT NULL,
    __tsid BIGINT UNSIGNED NOT NULL,
    greptime_metric STRING,
    greptime_timestamp TIMESTAMP(9) TIME INDEX,
    greptime_value DOUBLE,
    PRIMARY KEY (__table_id, __tsid, greptime_metric)
) WITH ('append_mode'='true', 'primary_key_encoding'='sparse');
INSERT INTO exemplar_source_ids VALUES
    (42, 18446744073709551615, 'cpu', 20, 1.0),
    (42, 18446744073709551615, 'cpu', 20, 1.0),
    (43, 18446744073709551615, 'memory', 10, 2.0);
ADMIN FLUSH_TABLE('exemplar_source_ids');
ALTER TABLE exemplar_source_ids ADD COLUMN job STRING PRIMARY KEY;
INSERT INTO exemplar_source_ids VALUES (42, 7, 'cpu', 30, 3.0, 'api');
