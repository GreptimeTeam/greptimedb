--- date_add ---
SELECT date_add('2023-12-06 07:39:46.222'::TIMESTAMP_MS, INTERVAL '5 day');

SELECT date_add('2023-12-06 07:39:46.222'::TIMESTAMP_MS, '5 day');

SELECT date_add('2023-12-06'::DATE, INTERVAL '3 month 5 day');

SELECT date_add('2023-12-06'::DATE, '3 month 5 day');

--- date_sub ---
SELECT date_sub('2023-12-06 07:39:46.222'::TIMESTAMP_MS, INTERVAL '5 day');

SELECT date_sub('2023-12-06 07:39:46.222'::TIMESTAMP_MS, '5 day');

SELECT date_sub('2023-12-06'::DATE, INTERVAL '3 month 5 day');

SELECT date_sub('2023-12-06'::DATE, '3 month 5 day');

--- date_format ---
SELECT date_format('2023-12-06 07:39:46.222'::TIMESTAMP_MS, '%Y-%m-%d %H:%M:%S:%3f');

SELECT date_format('2023-12-06 07:39:46.222'::TIMESTAMP_S, '%Y-%m-%d %H:%M:%S:%3f');

--- datetime not supported yet ---
SELECT date_format('2023-12-06 07:39:46.222'::DATETIME, '%Y-%m-%d %H:%M:%S:%3f');

SELECT date_format('2023-12-06'::DATE, '%m-%d');

--- test date functions with table rows ---
CREATE TABLE dates(d DATE, ts timestamp time index);

INSERT INTO dates VALUES ('1992-01-01'::DATE, 1);

INSERT INTO dates VALUES ('1993-12-30'::DATE, 2);

INSERT INTO dates VALUES ('2023-12-06'::DATE, 3);

--- date_add ---
SELECT date_add(d, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_add(d, '1 year 2 month 3 day') from dates;

SELECT date_add(ts, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_add(ts, '1 year 2 month 3 day') from dates;

--- date_sub ---
SELECT date_sub(d, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_sub(d, '1 year 2 month 3 day') from dates;

SELECT date_sub(ts, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_sub(ts, '1 year 2 month 3 day') from dates;

--- date_format ---
SELECT date_format(d, '%Y-%m-%d %H:%M:%S:%3f') from dates;

SELECT date_format(ts, '%Y-%m-%d %H:%M:%S:%3f') from dates;

DROP TABLE dates;
