CREATE TABLE http_requests (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val DOUBLE,
  PRIMARY KEY(host, idc),
);

CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", http_requests);

SELECT source_table_names FROM information_schema.flows WHERE flow_name = 'calc_reqs';

SHOW CREATE TABLE cnt_reqs;

-- test if sink table is tql queryable
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", cnt_reqs);

INSERT INTO TABLE http_requests VALUES
    (now() - '17s'::interval, 'host1', 'idc1', 200),
    (now() - '17s'::interval, 'host2', 'idc1', 200),
    (now() - '17s'::interval, 'host3', 'idc2', 200),
    (now() - '17s'::interval, 'host4', 'idc2', 401),
    (now() - '13s'::interval, 'host1', 'idc1', 404),
    (now() - '13s'::interval, 'host2', 'idc1', 401),
    (now() - '13s'::interval, 'host3', 'idc2', 404),
    (now() - '13s'::interval, 'host4', 'idc2', 500),
    (now() - '7s'::interval, 'host1', 'idc1', 200),
    (now() - '7s'::interval, 'host2', 'idc1', 200),
    (now() - '7s'::interval, 'host3', 'idc2', 201),
    (now() - '7s'::interval, 'host4', 'idc2', 201),
    (now() - '3s'::interval, 'host1', 'idc1', 500),
    (now() - '3s'::interval, 'host2', 'idc1', 500),
    (now() - '3s'::interval, 'host3', 'idc2', 500),
    (now() - '3s'::interval, 'host4', 'idc2', 500);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_reqs');

-- too much indeterminsticity in the test, so just check that the flow is running
SELECT count(*) > 0 FROM cnt_reqs;

DROP FLOW calc_reqs;
DROP TABLE http_requests;
DROP TABLE cnt_reqs;

CREATE TABLE http_requests_two_vals (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val DOUBLE,
  valb DOUBLE,
  PRIMARY KEY(host, idc),
);

-- should failed with two value columns error
CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", http_requests_two_vals);

-- should failed with two value columns error
-- SQLNESS REPLACE id=[0-9]+ id=[REDACTED]
CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '5s') rate(http_requests_two_vals[5m]);

SHOW TABLES;

DROP TABLE http_requests_two_vals;

CREATE TABLE http_requests (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val DOUBLE,
  PRIMARY KEY(host, idc),
);

CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (0, 15, '5s') count_values("status_code", http_requests);

-- standalone&distributed have slightly different error message(distributed will print source error as well ("cannot convert float seconds to Duration: value is negative"))
-- so duplicate test into two
CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - now(), now()-(now()+'15s'::interval), '5s') count_values("status_code", http_requests);

CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - now(), now()-now()+'15s'::interval, '5s') count_values("status_code", http_requests);


CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - now(), now()-(now()-'15s'::interval), '5s') count_values("status_code", http_requests);

SHOW CREATE TABLE cnt_reqs;

-- test if sink table is tql queryable
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", cnt_reqs);

INSERT INTO TABLE http_requests VALUES
    (0::Timestamp, 'host1', 'idc1', 200),
    (0::Timestamp, 'host2', 'idc1', 200),
    (0::Timestamp, 'host3', 'idc2', 200),
    (0::Timestamp, 'host4', 'idc2', 401),
    (5000::Timestamp, 'host1', 'idc1', 404),
    (5000::Timestamp, 'host2', 'idc1', 401),
    (5000::Timestamp, 'host3', 'idc2', 404),
    (5000::Timestamp, 'host4', 'idc2', 500),
    (10000::Timestamp, 'host1', 'idc1', 200),
    (10000::Timestamp, 'host2', 'idc1', 200),
    (10000::Timestamp, 'host3', 'idc2', 201),
    (10000::Timestamp, 'host4', 'idc2', 201),
    (15000::Timestamp, 'host1', 'idc1', 500),
    (15000::Timestamp, 'host2', 'idc1', 500),
    (15000::Timestamp, 'host3', 'idc2', 500),
    (15000::Timestamp, 'host4', 'idc2', 500);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_reqs');

SELECT * FROM cnt_reqs ORDER BY ts, status_code;

DROP FLOW calc_reqs;
DROP TABLE http_requests;
DROP TABLE cnt_reqs;

CREATE TABLE http_requests (
  ts timestamp(3) time index,
  val DOUBLE,
);

CREATE FLOW calc_rate SINK TO rate_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '30s') rate(http_requests[5m]);

SHOW CREATE TABLE rate_reqs;

-- test if sink table is tql queryable
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", rate_reqs);

INSERT INTO TABLE http_requests VALUES
    (now() - '1m'::interval, 0),
    (now() - '30s'::interval, 1),
    (now(), 2);

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_rate');

SELECT count(*) > 0 FROM rate_reqs;

DROP FLOW calc_rate;
DROP TABLE http_requests;
DROP TABLE rate_reqs;

CREATE TABLE http_requests_total (
    host STRING,
    job STRING,
    instance STRING,
    byte DOUBLE,
    ts TIMESTAMP TIME INDEX,
    PRIMARY KEY (host, job, instance)
);

CREATE FLOW calc_rate 
SINK TO rate_reqs 
EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '30s') rate(http_requests_total{job="my_service"}[1m]);

SHOW CREATE TABLE rate_reqs;

-- test if sink table is tql queryable
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", rate_reqs);

INSERT INTO TABLE http_requests_total VALUES
    ('localhost', 'my_service', 'instance1', 100, now() - '1min'::interval),
    ('localhost', 'my_service', 'instance1', 200, now() - '45s'::interval),
    ('remotehost', 'my_service', 'instance1', 300, now() - '30s'::interval),
    ('remotehost', 'their_service', 'instance1', 300, now() - '15s'::interval),
    ('localhost', 'my_service', 'instance1', 400, now());

-- SQLNESS REPLACE (ADMIN\sFLUSH_FLOW\('\w+'\)\s+\|\n\+-+\+\n\|\s+)[0-9]+\s+\| $1 FLOW_FLUSHED  |
ADMIN FLUSH_FLOW('calc_rate');

SELECT count(*)>0 FROM rate_reqs;

CREATE FLOW calc_rate_by_matcher
SINK TO rate_reqs_by_matcher
EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '30s') rate({__name__="http_requests_total",job="my_service"}[1m]);

SELECT source_table_names FROM information_schema.flows WHERE flow_name = 'calc_rate_by_matcher';

DROP FLOW calc_rate_by_matcher;
DROP TABLE rate_reqs_by_matcher;

DROP FLOW calc_rate;
DROP TABLE http_requests_total;
DROP TABLE rate_reqs;
