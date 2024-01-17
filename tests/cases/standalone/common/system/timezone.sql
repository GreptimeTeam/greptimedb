--- tests for timezone ---
SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

CREATE TABLE test(d double, ts timestamp_ms time index);

INSERT INTO test values
       (1, '2024-01-01 00:00:00'),
       (2, '2024-01-02 08:00:00'),
       (3, '2024-01-03 16:00:00'),
       (4, '2024-01-04 00:00:00');

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';


--- UTC+8 ---
SET TIME_ZONE = '+8:00';

SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';

--- UTC-8 ---
SET TIME_ZONE = '-8:00';

SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';

drop table test;

-- revert timezone to UTC
SET TIME_ZONE = 'UTC';

SHOW VARIABLES time_zone;
