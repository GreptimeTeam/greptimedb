-- Keep the native JSON2 type unknown so schema encoding uses the Null form.
CREATE TABLE unknown_json2 (ts TIMESTAMP TIME INDEX, j JSON2)
WITH ('append_mode' = 'true', 'memtable.type' = 'bulk');

INSERT INTO unknown_json2 VALUES (0, NULL);

ADMIN FLUSH_TABLE('unknown_json2');
