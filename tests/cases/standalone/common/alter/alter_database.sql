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

ALTER DATABASE alter_database SET 'compaction.type'='twcs';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'compaction.twcs.time_window'='2h';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'compaction.twcs.trigger_file_num'='8';

ALTER DATABASE alter_database SET 'compaction.twcs.max_output_file_size'='512MB';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'compaction.twcs.time_window'='1d';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database UNSET 'compaction.twcs.trigger_file_num';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database UNSET 'compaction.twcs.time_window';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'compaction.twcs.fallback_to_local'='true';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database UNSET 'compaction.type';

ALTER DATABASE alter_database UNSET 'compaction.twcs.max_output_file_size';

ALTER DATABASE alter_database UNSET 'compaction.twcs.fallback_to_local';

SHOW CREATE DATABASE alter_database;

-- SQLNESS ARG restart=true
SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database SET 'invalid.compaction.option'='value';

ALTER DATABASE alter_database SET 'ttl'='1h';

ALTER DATABASE alter_database SET 'compaction.type'='twcs';

ALTER DATABASE alter_database SET 'compaction.twcs.time_window'='30m';

SHOW CREATE DATABASE alter_database;

ALTER DATABASE alter_database UNSET 'ttl';

SHOW CREATE DATABASE alter_database;

DROP DATABASE alter_database;

