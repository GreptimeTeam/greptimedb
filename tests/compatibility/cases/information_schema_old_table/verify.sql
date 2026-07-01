SELECT table_schema, table_name, table_type, engine
FROM information_schema.tables
WHERE table_name = 't_information_schema_old_table'
ORDER BY table_schema;

SELECT column_name, data_type, semantic_type
FROM information_schema.columns
WHERE table_name = 't_information_schema_old_table'
ORDER BY table_schema, column_name;

SELECT count(*)
FROM information_schema.table_semantics
WHERE table_name = 't_information_schema_old_table';
