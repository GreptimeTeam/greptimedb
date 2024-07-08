SHOW DATABASES;

SHOW FULL DATABASES;

USE information_schema;

SHOW TABLES;

SHOW TABLES LIKE 'tables';

SHOW FULL TABLES;

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) DATETIME
SHOW TABLE STATUS;

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) DATETIME
SHOW TABLE STATUS LIKE 'tables';

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) DATETIME
SHOW TABLE STATUS WHERE Name = 'tables';

-- SQLNESS REPLACE (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}) DATETIME
SHOW TABLE STATUS from public;

USE public;
