CREATE DATABASE alter_database;

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='10s';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='20s';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'ttl'='😁';

ALTER DATABASE alter_database SET '🕶️'='1s';

ALTER DATABASE alter_database SET 'ttl'='40s';

ALTER DATABASE alter_database UNSET 'ttl';

ALTER DATABASE alter_database UNSET '🕶️';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

DROP DATABASE alter_database;

