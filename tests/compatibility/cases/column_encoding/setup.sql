CREATE TABLE legacy_spans (ts TIMESTAMP TIME INDEX, span_id STRING, ts_end TIMESTAMP(9));
INSERT INTO legacy_spans VALUES (1, 'a1', 1000), (2, 'a2', 2000);
ADMIN FLUSH_TABLE('legacy_spans');
