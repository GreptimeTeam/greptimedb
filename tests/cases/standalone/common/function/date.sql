SELECT date_add('2023-12-06 07:39:46.222726'::TIMESTAMP_MS, INTERVAL '5 day');

SELECT date_add('2023-12-06'::DATE, INTERVAL '3 month 5 day');

CREATE TABLE dates(d DATE, ts timestamp time index);

INSERT INTO dates VALUES ('1992-01-01'::DATE, 1);

INSERT INTO dates VALUES ('1993-12-30'::DATE, 2);

INSERT INTO dates VALUES ('2023-12-06'::DATE, 3);

SELECT date_add(d, INTERVAL '1 year 2 month 3 day') from dates;

DROP TABLE dates;
