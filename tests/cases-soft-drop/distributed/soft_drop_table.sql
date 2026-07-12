CREATE DATABASE soft_drop_lifecycle;

USE soft_drop_lifecycle;

CREATE TABLE lifecycle_table (
  host STRING,
  reading INT,
  ts TIMESTAMP TIME INDEX
);

INSERT INTO lifecycle_table VALUES ('original', 42, 1000);

DROP TABLE lifecycle_table;

SELECT object_id > 0 AS has_object_id,
       object_type,
       original_catalog_name,
       original_schema_name,
       original_object_name,
       purge_status,
       restorable,
       dropped_at IS NOT NULL AS has_dropped_at,
       dropped_by,
       retention_expires_at IS NOT NULL AS has_retention_expires_at,
       retention_expires_at > dropped_at AS deadline_after_drop,
       restored_at,
       restored_by
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'lifecycle_table';

SELECT COUNT(*) AS live_table_count
FROM information_schema.tables
WHERE table_catalog = 'greptime'
  AND table_schema = 'soft_drop_lifecycle'
  AND table_name = 'lifecycle_table';

SELECT COUNT(*) AS tombstone_count
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'lifecycle_table';

UNDROP TABLE greptime.soft_drop_lifecycle.lifecycle_table;

SELECT host, reading, ts FROM lifecycle_table;

DROP TABLE lifecycle_table;

ADMIN purge_table('lifecycle_table');

SELECT COUNT(*) AS tombstone_count
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'lifecycle_table';

UNDROP TABLE greptime.soft_drop_lifecycle.lifecycle_table;

CREATE TABLE same_name (
  generation STRING,
  reading INT,
  ts TIMESTAMP TIME INDEX
);

INSERT INTO same_name VALUES ('old', 1, 2000);

DROP TABLE same_name;

CREATE TABLE same_name (
  generation STRING,
  reading INT,
  ts TIMESTAMP TIME INDEX
);

INSERT INTO same_name VALUES ('new', 2, 3000);

SELECT object_type,
       original_catalog_name,
       original_schema_name,
       original_object_name,
       purge_status,
       restorable
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'same_name';

UNDROP TABLE soft_drop_lifecycle.same_name;

DROP TABLE same_name;

ADMIN purge_table('greptime.soft_drop_lifecycle.same_name');

SELECT COUNT(*) AS tombstone_count
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'same_name';

SELECT generation, reading, ts FROM same_name;

DROP TABLE same_name;

SELECT COUNT(*) AS tombstone_count
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle'
  AND original_object_name = 'same_name';

UNDROP TABLE same_name;

SELECT generation, reading, ts FROM same_name;

DROP TABLE same_name;

ADMIN purge_table('soft_drop_lifecycle.same_name');

SELECT COUNT(*) AS tombstone_count
FROM information_schema.recycle_bin
WHERE original_schema_name = 'soft_drop_lifecycle';

USE public;

DROP DATABASE soft_drop_lifecycle;
