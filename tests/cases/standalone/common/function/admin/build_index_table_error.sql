-- Test error cases for ADMIN BUILD_INDEX

-- Error: wrong number of args (0 args, expected 1)
ADMIN BUILD_INDEX();

-- Error: wrong argument type (Int64, expected Utf8)
ADMIN BUILD_INDEX(1);

-- Error: table does not exist
ADMIN BUILD_INDEX('non_existent_table');
