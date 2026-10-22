-- The same results must hold with experimental_metadata_count enabled or disabled.
CREATE TABLE metadata_count_test (
  host STRING,
  ts TIMESTAMP TIME INDEX,
  v BIGINT,
  PRIMARY KEY (host)
) WITH ('compaction.type' = 'twcs', 'compaction.twcs.trigger_file_num' = '2');

INSERT INTO metadata_count_test VALUES ('a', 1000, 1), ('a', 2000, NULL), ('a', 3000, 3);
SELECT count(*) FROM metadata_count_test;
ADMIN FLUSH_TABLE('metadata_count_test');
INSERT INTO metadata_count_test VALUES ('a', 1000, 11), ('a', 4000, 4);
ADMIN FLUSH_TABLE('metadata_count_test');
DELETE FROM metadata_count_test WHERE host = 'a' AND ts = 3000;
ADMIN FLUSH_TABLE('metadata_count_test');
SELECT count(*) FROM metadata_count_test;
ADMIN COMPACT_TABLE('metadata_count_test');
SELECT count(*) FROM metadata_count_test;
SELECT count(*) FROM metadata_count_test WHERE ts >= 1000 AND ts < 5000;
SELECT count(*) FROM metadata_count_test WHERE ts < 3000;
SELECT count(*) FROM metadata_count_test WHERE ts = 1000 OR ts = 4000;
SELECT count(*) FROM metadata_count_test WHERE v > 3;
SELECT count(v), count(DISTINCT ts) FROM metadata_count_test;
INSERT INTO metadata_count_test VALUES ('a', 1000, 111), ('a', 5000, 5);
SELECT count(*) FROM metadata_count_test;
DELETE FROM metadata_count_test WHERE host = 'a' AND ts = 1000;
SELECT count(*) FROM metadata_count_test;
DROP TABLE metadata_count_test;
