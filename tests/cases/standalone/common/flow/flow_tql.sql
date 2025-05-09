CREATE TABLE http_requests (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val BIGINT,
  PRIMARY KEY(host, idc),
);

CREATE FLOW calc_reqs SINK TO cnt_reqs AS
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

SELECT "count(http_requests.val)" as cnt, status_code FROM cnt_reqs ORDER BY ts ASC;

DROP FLOW calc_reqs;
DROP TABLE http_requests;
DROP TABLE cnt_reqs;
