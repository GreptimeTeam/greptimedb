CREATE TABLE IF NOT EXISTS timestamp (t TIMESTAMP, ts TIMESTAMP time index DEFAULT current_timestamp());

INSERT INTO timestamp VALUES ('2008-01-01 00:00:01', 1), (NULL, 2), ('2007-01-01 00:00:01', 3), ('2008-02-01 00:00:01', 4), ('2008-01-02 00:00:01', 5), ('2008-01-01 10:00:00', 6), ('2008-01-01 00:10:00', 7), ('2008-01-01 00:00:10', 8);

SELECT timestamp '2017-07-23 13:10:11';

SELECT timestamp '2017-07-23T13:10:11',timestamp '2017-07-23T13:10:11Z';

SELECT timestamp '    2017-07-23     13:10:11    ';

SELECT timestamp '    2017-07-23     13:10:11    AA';

SELECT timestamp 'AA2017-07-23 13:10:11';

SELECT timestamp '2017-07-23A13:10:11';

SELECT t FROM timestamp ORDER BY t;

SELECT MIN(t) FROM timestamp;

SELECT MAX(t) FROM timestamp;

SELECT SUM(t) FROM timestamp;

SELECT AVG(t) FROM timestamp;

SELECT t+t FROM timestamp;

SELECT t*t FROM timestamp;

SELECT t/t FROM timestamp;

SELECT t%t FROM timestamp;

-- TODO(dennis): It can't run on distributed mode, uncomment it when the issue is fixed: https://github.com/GreptimeTeam/greptimedb/issues/2071 --
-- SELECT t-t FROM timestamp; --

SELECT EXTRACT(YEAR from TIMESTAMP '1992-01-01 01:01:01');

SELECT EXTRACT(YEAR from TIMESTAMP '1992-01-01 01:01:01'::DATE);

SELECT (TIMESTAMP '1992-01-01 01:01:01')::DATE;

SELECT (TIMESTAMP '1992-01-01 01:01:01')::TIME;

SELECT t::DATE FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1;

SELECT t::TIME FROM timestamp WHERE EXTRACT(YEAR from t)=2007 ORDER BY 1;

SELECT (DATE '1992-01-01')::TIMESTAMP;

SELECT TIMESTAMP '2008-01-01 00:00:01.5'::VARCHAR;

SELECT TIMESTAMP '-8-01-01 00:00:01.5'::VARCHAR;

SELECT TIMESTAMP '100000-01-01 00:00:01.5'::VARCHAR;

DROP TABLE timestamp;
