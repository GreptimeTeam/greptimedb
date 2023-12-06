SELECT date_add('2023-12-06 07:39:46.222'::TIMESTAMP_MS, INTERVAL '5 day');

SELECT date_add('2023-12-06 07:39:46.222'::TIMESTAMP_MS, '5 day');

SELECT date_add('2023-12-06'::DATE, INTERVAL '3 month 5 day');

SELECT date_add('2023-12-06'::DATE, '3 month 5 day');

SELECT date_sub('2023-12-06 07:39:46.222'::TIMESTAMP_MS, INTERVAL '5 day');

SELECT date_sub('2023-12-06 07:39:46.222'::TIMESTAMP_MS, '5 day');

SELECT date_sub('2023-12-06'::DATE, INTERVAL '3 month 5 day');

SELECT date_sub('2023-12-06'::DATE, '3 month 5 day');


CREATE TABLE dates(d DATE, ts timestamp time index);

INSERT INTO dates VALUES ('1992-01-01'::DATE, 1);

INSERT INTO dates VALUES ('1993-12-30'::DATE, 2);

INSERT INTO dates VALUES ('2023-12-06'::DATE, 3);

SELECT date_add(d, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_add(d, '1 year 2 month 3 day') from dates;

SELECT date_add(ts, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_add(ts, '1 year 2 month 3 day') from dates;

SELECT date_sub(d, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_sub(d, '1 year 2 month 3 day') from dates;

SELECT date_sub(ts, INTERVAL '1 year 2 month 3 day') from dates;

SELECT date_sub(ts, '1 year 2 month 3 day') from dates;

DROP TABLE dates;
