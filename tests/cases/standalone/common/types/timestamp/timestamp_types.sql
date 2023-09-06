--description: Test TIMESTAMP types

CREATE TABLE IF NOT EXISTS timestamp (sec TIMESTAMP_S, milli TIMESTAMP_MS,micro TIMESTAMP_US, nano TIMESTAMP_NS, ts TIMESTAMP TIME INDEX);

INSERT INTO timestamp VALUES ('2008-01-01 00:00:01','2008-01-01 00:00:01.594','2008-01-01 00:00:01.88926','2008-01-01 00:00:01.889268321', 1);

SELECT * from timestamp;

SELECT extract(YEAR from sec),extract( YEAR from milli),extract(YEAR from nano) from timestamp;

SELECT nano::TIMESTAMP, milli::TIMESTAMP,sec::TIMESTAMP from timestamp;

SELECT micro::TIMESTAMP_S as m1, micro::TIMESTAMP_MS as m2,micro::TIMESTAMP_NS as m3 from timestamp;


INSERT INTO timestamp VALUES ('2008-01-01 00:00:51','2008-01-01 00:00:01.894','2008-01-01 00:00:01.99926','2008-01-01 00:00:01.999268321', 2);

INSERT INTO timestamp VALUES ('2008-01-01 00:00:11','2008-01-01 00:00:01.794','2008-01-01 00:00:01.98926','2008-01-01 00:00:01.899268321', 3);


select '90000-01-19 03:14:07.999999'::TIMESTAMP_US::TIMESTAMP_NS;

select sec::DATE from timestamp;

select milli::DATE from timestamp;

select nano::DATE from timestamp;

select sec::TIME from timestamp;

select milli::TIME from timestamp;

select nano::TIME from timestamp;

select sec::TIMESTAMP_MS from timestamp;

select sec::TIMESTAMP_NS from timestamp;

select milli::TIMESTAMP_SEC from timestamp;

select milli::TIMESTAMP_NS from timestamp;

select nano::TIMESTAMP_SEC from timestamp;

select nano::TIMESTAMP_MS from timestamp;

select sec from timestamp order by sec;

select milli from timestamp order by milli;

select nano from timestamp order by nano;

INSERT INTO timestamp VALUES ('2008-01-01 00:00:51','2008-01-01 00:00:01.894','2008-01-01 00:00:01.99926','2008-01-01 00:00:01.999268321', 4);

INSERT INTO timestamp VALUES ('2008-01-01 00:00:11','2008-01-01 00:00:01.794','2008-01-01 00:00:01.98926','2008-01-01 00:00:01.899268321', 5);

select count(*), nano from timestamp group by nano order by nano;

select count(*), sec from timestamp group by sec order by sec;

select count(*), milli from timestamp group by milli order by milli;

CREATE TABLE IF NOT EXISTS timestamp_two (sec TIMESTAMP_S, milli TIMESTAMP_MS,micro TIMESTAMP_US, nano TIMESTAMP_NS TIME INDEX);

INSERT INTO timestamp_two VALUES ('2008-01-01 00:00:11','2008-01-01 00:00:01.794','2008-01-01 00:00:01.98926','2008-01-01 00:00:01.899268321');

select timestamp.sec from timestamp inner join  timestamp_two on (timestamp.sec = timestamp_two.sec);

select timestamp.milli from timestamp inner join  timestamp_two on (timestamp.milli = timestamp_two.milli);

select timestamp.nano from timestamp inner join  timestamp_two on (timestamp.nano = timestamp_two.nano);

select '2008-01-01 00:00:11'::TIMESTAMP_US = '2008-01-01 00:00:11'::TIMESTAMP_MS;

select '2008-01-01 00:00:11'::TIMESTAMP_US = '2008-01-01 00:00:11'::TIMESTAMP_NS;

select '2008-01-01 00:00:11'::TIMESTAMP_US = '2008-01-01 00:00:11'::TIMESTAMP_S;

select '2008-01-01 00:00:11.1'::TIMESTAMP_US = '2008-01-01 00:00:11'::TIMESTAMP_MS;

select '2008-01-01 00:00:11.1'::TIMESTAMP_US = '2008-01-01 00:00:11'::TIMESTAMP_NS;

select '2008-01-01 00:00:11.1'::TIMESTAMP_US = '2008-01-01 00:00:11.1'::TIMESTAMP_S;

select '2008-01-01 00:00:11.1'::TIMESTAMP_MS = '2008-01-01 00:00:11'::TIMESTAMP_NS;

select '2008-01-01 00:00:11.1'::TIMESTAMP_MS = '2008-01-01 00:00:11'::TIMESTAMP_S;

select '2008-01-01 00:00:11.1'::TIMESTAMP_NS = '2008-01-01 00:00:11'::TIMESTAMP_S;

select '2008-01-01 00:00:11'::TIMESTAMP_MS = '2008-01-01 00:00:11'::TIMESTAMP_NS;

select '2008-01-01 00:00:11'::TIMESTAMP_MS = '2008-01-01 00:00:11'::TIMESTAMP_S;

select '2008-01-01 00:00:11'::TIMESTAMP_NS = '2008-01-01 00:00:11'::TIMESTAMP_S;

DROP TABLE timestamp;

DROP TABLE timestamp_two;
