SELECT ts, j FROM unknown_json2 ORDER BY ts;

-- Decode the previous version's unknown type and evolve it on a new write.
INSERT INTO unknown_json2 VALUES
  (1, '{"items":[1,"two",null],"ok":true}'),
  (2, NULL);

-- Exercise schema creation with this version's encoding as well.
CREATE TABLE new_unknown_json2 (ts TIMESTAMP TIME INDEX, j JSON2)
WITH ('append_mode' = 'true', 'memtable.type' = 'bulk');

INSERT INTO new_unknown_json2 VALUES (0, NULL);

ADMIN FLUSH_TABLE('unknown_json2');
ADMIN FLUSH_TABLE('new_unknown_json2');

-- SQLNESS ARG restart=true
SELECT ts, j FROM new_unknown_json2 ORDER BY ts;

INSERT INTO new_unknown_json2 VALUES (1, '{"items":[1,"two",null],"ok":true}');

SELECT ts, j FROM unknown_json2 ORDER BY ts;
SELECT ts, j FROM new_unknown_json2 ORDER BY ts;
