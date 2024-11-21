CREATE DATABASE alter_database;

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='10s';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='20s';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database UNSET 'ttl';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

DROP DATABASE alter_database;

