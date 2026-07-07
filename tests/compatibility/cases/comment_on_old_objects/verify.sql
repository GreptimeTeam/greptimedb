COMMENT ON TABLE compat_comment_table IS 'upgraded table comment';

COMMENT ON COLUMN compat_comment_table.val IS 'upgraded value column comment';

COMMENT ON FLOW compat_comment_flow IS 'upgraded flow comment';

SELECT table_schema, table_name, table_comment
FROM information_schema.tables
WHERE table_name = 'compat_comment_table'
ORDER BY table_schema, table_name;

SELECT table_schema, table_name, column_name, column_comment
FROM information_schema.columns
WHERE table_name = 'compat_comment_table'
  AND column_name = 'val'
ORDER BY table_schema, table_name, column_name;

SELECT flow_name, comment
FROM information_schema.flows
WHERE flow_name = 'compat_comment_flow'
ORDER BY flow_name;
