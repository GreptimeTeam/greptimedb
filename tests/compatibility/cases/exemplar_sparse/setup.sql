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
