-- Test: COMMENT ON TABLE add & remove
CREATE TABLE comment_table_test (
	pk INT,
	val DOUBLE,
	ts TIMESTAMP TIME INDEX,
	PRIMARY KEY(pk)
);

-- Add table comment
COMMENT ON TABLE comment_table_test IS 'table level description';
SHOW CREATE TABLE comment_table_test;
SELECT table_comment
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'comment_table_test';

-- Remove table comment
COMMENT ON TABLE comment_table_test IS NULL;
SHOW CREATE TABLE comment_table_test;
SELECT table_comment
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name = 'comment_table_test';

DROP TABLE comment_table_test;

-- Test: COMMENT ON COLUMN add & remove
CREATE TABLE comment_column_test (
	pk INT,
	val DOUBLE,
	ts TIMESTAMP TIME INDEX,
	PRIMARY KEY(pk)
);

-- Add column comment
COMMENT ON COLUMN comment_column_test.val IS 'value column description';
SHOW CREATE TABLE comment_column_test;
SELECT column_comment
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'comment_column_test'
  AND column_name = 'val';

-- Remove column comment
COMMENT ON COLUMN comment_column_test.val IS NULL;
SHOW CREATE TABLE comment_column_test;
SELECT column_comment
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'comment_column_test'
  AND column_name = 'val';

DROP TABLE comment_column_test;

-- Test: COMMENT ON FLOW add & remove
-- Prepare source & sink tables
CREATE TABLE flow_source_comment_test (
	desc_str STRING,
	ts TIMESTAMP TIME INDEX
);

CREATE TABLE flow_sink_comment_test (
	desc_str STRING,
	ts TIMESTAMP TIME INDEX
);

CREATE FLOW flow_comment_test
SINK TO flow_sink_comment_test
AS
SELECT desc_str, ts FROM flow_source_comment_test;

-- Add flow comment
COMMENT ON FLOW flow_comment_test IS 'flow level description';
SHOW CREATE FLOW flow_comment_test;
SELECT comment
FROM information_schema.flows
WHERE flow_name = 'flow_comment_test';

-- Remove flow comment
COMMENT ON FLOW flow_comment_test IS NULL;
SHOW CREATE FLOW flow_comment_test;
SELECT comment
FROM information_schema.flows
WHERE flow_name = 'flow_comment_test';

DROP FLOW flow_comment_test;
DROP TABLE flow_source_comment_test;
DROP TABLE flow_sink_comment_test;
