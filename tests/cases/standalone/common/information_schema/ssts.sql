DESC TABLE information_schema.ssts_manifest;

DESC TABLE information_schema.ssts_storage;

DESC TABLE information_schema.ssts_index_meta;

CREATE TABLE sst_case (
  a INT PRIMARY KEY INVERTED INDEX,
  b STRING SKIPPING INDEX,
  c STRING FULLTEXT INDEX,
  ts TIMESTAMP TIME INDEX,
)
PARTITION ON COLUMNS (a) (
  a < 1000,
  a >= 1000 AND a < 2000,
  a >= 2000
);

INSERT INTO sst_case VALUES
  (500, 'a', 'a', 1),
  (1500, 'b', 'b', 2),
  (2500, 'c', 'c', 3);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by file_path;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
-- SQLNESS REPLACE (\{"(bloom|fulltext|inverted)":.*\}) <META_JSON>
SELECT *,
  CASE index_type
    WHEN 'bloom_filter' THEN concat(
      'bloom_rows_per_segment=', CAST(json_get_int(parse_json(meta_json), 'bloom.rows_per_segment') AS STRING),
      ',bloom_segment_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.segment_count') AS STRING),
      ',bloom_row_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.row_count') AS STRING),
      ',bloom_filter_size=', CAST(json_get_int(parse_json(meta_json), 'bloom.bloom_filter_size') AS STRING))
    WHEN 'fulltext_bloom' THEN concat(
      'bloom_rows_per_segment=', CAST(json_get_int(parse_json(meta_json), 'bloom.rows_per_segment') AS STRING),
      ',bloom_segment_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.segment_count') AS STRING),
      ',bloom_row_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.row_count') AS STRING),
      ',bloom_filter_size=', CAST(json_get_int(parse_json(meta_json), 'bloom.bloom_filter_size') AS STRING),
      ',fulltext_analyzer=', json_get_string(parse_json(meta_json), 'fulltext.analyzer'),
      ',fulltext_case_sensitive=', CAST(json_get_bool(parse_json(meta_json), 'fulltext.case_sensitive') AS STRING))
    WHEN 'inverted' THEN concat(
      'inverted_bitmap_type=', json_get_string(parse_json(meta_json), 'inverted.bitmap_type'),
      ',inverted_base_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.base_offset') AS STRING),
      ',inverted_index_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.inverted_index_size') AS STRING),
      ',inverted_relative_fst_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.relative_fst_offset') AS STRING),
      ',inverted_fst_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.fst_size') AS STRING),
      ',inverted_relative_null_bitmap_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.relative_null_bitmap_offset') AS STRING),
      ',inverted_null_bitmap_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.null_bitmap_size') AS STRING),
      ',inverted_segment_row_count=', CAST(json_get_int(parse_json(meta_json), 'inverted.segment_row_count') AS STRING),
      ',inverted_total_row_count=', CAST(json_get_int(parse_json(meta_json), 'inverted.total_row_count') AS STRING))
  END AS index_meta_summary
FROM information_schema.ssts_index_meta
ORDER BY index_type, target_type, target_key,
  json_get_int(parse_json(meta_json), 'bloom.row_count'),
  json_get_int(parse_json(meta_json), 'inverted.total_row_count'),
  region_number, region_sequence, file_id;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

INSERT INTO sst_case VALUES
  (24, 'foo', 'foo', 100),
  (124, 'bar', 'bar', 200),
  (1024, 'baz', 'baz', 300);

ADMIN FLUSH_TABLE('sst_case');

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
SELECT * FROM information_schema.ssts_manifest order by region_id, sequence;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+/index/<UUID>\.puffin) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>/index/<UUID>.puffin
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
-- SQLNESS REPLACE (/public/\d+) /public/<TABLE_ID>
-- SQLNESS REPLACE (\{"(bloom|fulltext|inverted)":.*\}) <META_JSON>
SELECT *,
  CASE index_type
    WHEN 'bloom_filter' THEN concat(
      'bloom_rows_per_segment=', CAST(json_get_int(parse_json(meta_json), 'bloom.rows_per_segment') AS STRING),
      ',bloom_segment_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.segment_count') AS STRING),
      ',bloom_row_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.row_count') AS STRING),
      ',bloom_filter_size=', CAST(json_get_int(parse_json(meta_json), 'bloom.bloom_filter_size') AS STRING))
    WHEN 'fulltext_bloom' THEN concat(
      'bloom_rows_per_segment=', CAST(json_get_int(parse_json(meta_json), 'bloom.rows_per_segment') AS STRING),
      ',bloom_segment_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.segment_count') AS STRING),
      ',bloom_row_count=', CAST(json_get_int(parse_json(meta_json), 'bloom.row_count') AS STRING),
      ',bloom_filter_size=', CAST(json_get_int(parse_json(meta_json), 'bloom.bloom_filter_size') AS STRING),
      ',fulltext_analyzer=', json_get_string(parse_json(meta_json), 'fulltext.analyzer'),
      ',fulltext_case_sensitive=', CAST(json_get_bool(parse_json(meta_json), 'fulltext.case_sensitive') AS STRING))
    WHEN 'inverted' THEN concat(
      'inverted_bitmap_type=', json_get_string(parse_json(meta_json), 'inverted.bitmap_type'),
      ',inverted_base_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.base_offset') AS STRING),
      ',inverted_index_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.inverted_index_size') AS STRING),
      ',inverted_relative_fst_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.relative_fst_offset') AS STRING),
      ',inverted_fst_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.fst_size') AS STRING),
      ',inverted_relative_null_bitmap_offset=', CAST(json_get_int(parse_json(meta_json), 'inverted.relative_null_bitmap_offset') AS STRING),
      ',inverted_null_bitmap_size=', CAST(json_get_int(parse_json(meta_json), 'inverted.null_bitmap_size') AS STRING),
      ',inverted_segment_row_count=', CAST(json_get_int(parse_json(meta_json), 'inverted.segment_row_count') AS STRING),
      ',inverted_total_row_count=', CAST(json_get_int(parse_json(meta_json), 'inverted.total_row_count') AS STRING))
  END AS index_meta_summary
FROM information_schema.ssts_index_meta
ORDER BY index_type, target_type, target_key,
  json_get_int(parse_json(meta_json), 'bloom.row_count'),
  json_get_int(parse_json(meta_json), 'inverted.total_row_count'),
  region_number, region_sequence, file_id;

-- SQLNESS REPLACE (\s+\d+\s+) <NUM>
-- SQLNESS REPLACE ([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}) <UUID>
-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?[[:blank:]]*) <DATETIME>
-- SQLNESS REPLACE (/public/\d+/\d+_\d+) /public/<TABLE_ID>/<REGION_ID>_<REGION_NUMBER>
SELECT * FROM information_schema.ssts_storage order by file_path;

DROP TABLE sst_case;
