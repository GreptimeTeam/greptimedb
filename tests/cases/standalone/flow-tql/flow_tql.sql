CREATE TABLE http_requests (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val DOUBLE,
  PRIMARY KEY(host, idc),
);

CREATE FLOW calc_reqs SINK TO cnt_reqs EVAL INTERVAL '1m' AS
TQL EVAL (now() - '1m'::interval, now(), '5s') count_values("status_code", http_requests);

SHOW CREATE TABLE cnt_reqs;

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

SELECT val, ts, status_code FROM cnt_reqs ORDER BY ts, status_code;

DROP FLOW calc_reqs;
DROP TABLE http_requests;
DROP TABLE cnt_reqs;
