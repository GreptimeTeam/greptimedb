-- common test
SELECT INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6 seconds 100 microseconds';

SELECT INTERVAL '1.5 year';

SELECT INTERVAL '-2 months';

SELECT '3 hours'::INTERVAL;

SELECT INTERVAL '1 year 2 months 3 days 4 hours' + INTERVAL '1 year';

SELECT INTERVAL '1 year 2 months 3 days 4 hours' - INTERVAL '1 year';

SELECT INTERVAL '6 years' * 2;

SELECT INTERVAL '6 years' / 2;

SELECT INTERVAL '6 years' = INTERVAL '72 months';

SELECT arrow_typeof(INTERVAL '1 month');

-- table with interval type test
-- breaking change from #5422 table do not support interval type will raise an error
CREATE TABLE IF NOT EXISTS intervals(
  ts TIMESTAMP TIME INDEX,
  interval_value INTERVAL,
);

-- Interval shortened names
SELECT INTERVAL '55h';

SELECT INTERVAL '-2mon';

SELECT INTERVAL '-1h5m';

SELECT INTERVAL '-1h-5m';

SELECT INTERVAL '1y2w3d4h';

SELECT '3y2mon'::INTERVAL;

SELECT INTERVAL '7 days' - INTERVAL '1d';

SELECT INTERVAL '2h' + INTERVAL '1h';

-- Interval ISO 8601
SELECT INTERVAL 'p3y3m700dt133h17m36.789s';

SELECT INTERVAL '-P3Y3M700DT133H17M36.789S';

SELECT 'P3Y3M700DT133H17M36.789S'::INTERVAL;

SELECT INTERVAL '2h' + INTERVAL 'P3Y3M700DT133H17M36.789S';

select '2022-01-01T00:00:01'::timestamp + '1 days'::interval;

select '2022-01-01T00:00:01'::timestamp + '2 days'::interval;

select '2022-01-01T00:00:01'::timestamp - '1 days'::interval;

select '2022-01-01T00:00:01'::timestamp - '2 days'::interval;

select '2022-01-01T00:00:01'::timestamp + '1 month'::interval;

select '2022-01-01T00:00:01'::timestamp + '2 months'::interval;

select '2022-01-01T00:00:01'::timestamp + '1 year'::interval;

select '2023-01-01T00:00:01'::timestamp + '2 years'::interval;

-- DATE + INTERVAL
SELECT DATE '2000-10-30' + '1 days'::interval;

SELECT DATE '2000-10-30' + '2 months'::interval;

SELECT DATE '2000-10-30' + '2 years'::interval;

-- DATE - INTERVAL
SELECT DATE '2000-10-30' - '1 days'::interval;

SELECT DATE '2000-10-30' - '2 months'::interval;

SELECT DATE '2000-10-30' - '2 years'::interval;
