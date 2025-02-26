SHOW DATABASES;

SHOW FULL DATABASES;

USE information_schema;

SHOW TABLES;

SHOW TABLES LIKE 'tables';

SHOW FULL TABLES;

-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
SHOW TABLE STATUS;

-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
SHOW TABLE STATUS LIKE 'tables';

-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
SHOW TABLE STATUS WHERE Name = 'tables';

-- SQLNESS REPLACE (\s[\-0-9T:\.]{15,}) DATETIME
-- SQLNESS REPLACE [\u0020\-]+
SHOW TABLE STATUS from public;

USE public;
