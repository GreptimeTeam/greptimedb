CREATE TABLE IF NOT EXISTS prune_field (
  ts TIMESTAMP TIME INDEX,
  `tag` UInt16,
  a UInt8,
  b UInt8,
PRIMARY KEY (`tag`)) ENGINE = mito WITH('merge_mode'='last_non_null');

insert into prune_field(ts, `tag`, a, b) values(0, 1, 1, null);

admin flush_table('prune_field');

insert into prune_field(ts, `tag`, a, b) values(0, 1, null, 1);

admin flush_table('prune_field');

select * from prune_field where a = 1;

select * from prune_field where b = 1;

select * from prune_field;

select * from prune_field where a = 1 and b = 1;

drop table prune_field;

-- A single flat SST can contain both the put and its delete marker.
CREATE TABLE prune_deleted_flat (
  host STRING, ts TIMESTAMP TIME INDEX,
  mem BIGINT NULL, note STRING NULL,
  PRIMARY KEY (host)
) WITH ('merge_mode' = 'last_non_null', 'sst_format' = 'flat');

INSERT INTO prune_deleted_flat VALUES ('h1', '2024-01-01 00:00:00', 2, 'b');
DELETE FROM prune_deleted_flat WHERE host = 'h1' AND ts = '2024-01-01 00:00:00';
ADMIN flush_table('prune_deleted_flat');

SELECT * FROM prune_deleted_flat;
SELECT * FROM prune_deleted_flat WHERE mem = 2;
SELECT * FROM prune_deleted_flat WHERE note = 'b';

DROP TABLE prune_deleted_flat;

-- Strict-window compaction can put separately flushed versions into one SST.
CREATE TABLE prune_deleted_pk (
  host STRING, ts TIMESTAMP TIME INDEX,
  mem BIGINT NULL, note STRING NULL,
  PRIMARY KEY (host)
) WITH ('merge_mode' = 'last_non_null', 'sst_format' = 'primary_key', 'memtable.type' = 'time_series');

INSERT INTO prune_deleted_pk VALUES ('h1', '2024-01-01 00:00:00', 2, 'b');
ADMIN flush_table('prune_deleted_pk');
DELETE FROM prune_deleted_pk WHERE host = 'h1' AND ts = '2024-01-01 00:00:00';
ADMIN flush_table('prune_deleted_pk');
INSERT INTO prune_deleted_pk VALUES ('h2', '2024-01-01 00:00:00', 3, 'c');
ADMIN flush_table('prune_deleted_pk');

SELECT COUNT(DISTINCT file_id) AS ssts FROM information_schema.ssts_manifest
WHERE visible AND table_id = (SELECT table_id FROM information_schema.tables
                             WHERE table_schema = 'public' AND table_name = 'prune_deleted_pk');
SELECT * FROM prune_deleted_pk WHERE mem = 2;
SELECT * FROM prune_deleted_pk WHERE note = 'b';

ADMIN compact_table('prune_deleted_pk', 'swcs', '86400');

SELECT COUNT(DISTINCT file_id) AS ssts FROM information_schema.ssts_manifest
WHERE visible AND table_id = (SELECT table_id FROM information_schema.tables
                             WHERE table_schema = 'public' AND table_name = 'prune_deleted_pk');
SELECT * FROM prune_deleted_pk;
SELECT * FROM prune_deleted_pk WHERE mem = 2;
SELECT * FROM prune_deleted_pk WHERE note = 'b';

DROP TABLE prune_deleted_pk;

-- -0.0 equals 0.0, so flushed rows holding either zero must keep matching.
CREATE TABLE prune_float_zero (ts TIMESTAMP TIME INDEX, v DOUBLE);

insert into prune_float_zero values (1, -0.0), (2, 0.0), (3, 1.0);

admin flush_table('prune_float_zero');

select ts from prune_float_zero where v = 0.0 order by ts;

select ts from prune_float_zero where v >= 0 order by ts;

select ts from prune_float_zero where v = -0.0 order by ts;

select ts from prune_float_zero where v in (0.0) order by ts;

drop table prune_float_zero;
