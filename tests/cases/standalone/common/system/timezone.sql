--- tests for timezone ---
SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

select timezone();

CREATE TABLE test(d double, ts timestamp_ms time index);

INSERT INTO test values
       (1, '2024-01-01 00:00:00'),
       (2, '2024-01-02 08:00:00'),
       (3, '2024-01-03 16:00:00'),
       (4, '2024-01-04 00:00:00'),
       (5, '2024-01-05 00:00:00+08:00');

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';

select date_format(ts, '%Y-%m-%d %H:%M:%S:%3f') from test;

select to_unixtime('2024-01-02 00:00:00');

select to_unixtime('2024-01-02T00:00:00+08:00');

--- UTC+8 ---
SET TIME_ZONE = '+8:00';

SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

select timezone();

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';

select date_format(ts, '%Y-%m-%d %H:%M:%S:%3f') from test;

select to_unixtime('2024-01-02 00:00:00');

select to_unixtime('2024-01-02 00:00:00+08:00');

--- UTC-8 ---
SET SESSION TIME_ZONE = '-8:00';

SHOW VARIABLES time_zone;

SHOW VARIABLES system_time_zone;

select timezone();

SELECT * from test;

SELECT * from test where ts >= '2024-01-02 08:00:00';

SELECT * from test where ts <= '2024-01-03 16:00:00';

select date_format(ts, '%Y-%m-%d %H:%M:%S:%3f') from test;

select to_unixtime('2024-01-02 00:00:00');

select to_unixtime('2024-01-02 00:00:00+08:00');

drop table test;

-- revert timezone to UTC
SET LOCAL TIME_ZONE = 'UTC';

SHOW VARIABLES time_zone;

select timezone();
