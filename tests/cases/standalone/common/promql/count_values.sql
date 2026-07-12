CREATE TABLE http_requests (
  ts timestamp(3) time index,
  host STRING,
  idc STRING,
  val BIGINT,
  PRIMARY KEY(host, idc),
);

INSERT INTO TABLE http_requests VALUES
    (0,     'host1', "idc1", 200),
    (0,     'host2', "idc1", 200),
    (0,     'host3', "idc2", 200),
    (0,     'host4', "idc2", 401),
    (5000,  'host1', "idc1", 404),
    (5000,  'host2', "idc1", 401),
    (5000,  'host3', "idc2", 404),
    (5000,  'host4', "idc2", 500),
    (10000, 'host1', "idc1", 200),
    (10000, 'host2', "idc1", 200),
    (10000, 'host3', "idc2", 201),
    (10000, 'host4', "idc2", 201),
    (15000, 'host1', "idc1", 500),
    (15000, 'host2', "idc1", 500),
    (15000, 'host3', "idc2", 500),
    (15000, 'host4', "idc2", 500);

TQL EVAL (0, 15, '5s') count_values("status_code", http_requests);

TQL EVAL (0, 15, '5s') count_values("status_code", http_requests) by (idc);

DROP TABLE http_requests;
