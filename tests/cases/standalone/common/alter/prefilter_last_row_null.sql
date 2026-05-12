CREATE TABLE prefilter_last_row_null(
    host STRING,
    ts TIMESTAMP TIME INDEX,
    cpu DOUBLE,
    PRIMARY KEY(host)
) ENGINE=mito;

INSERT INTO prefilter_last_row_null(host, ts, cpu) VALUES ('host1', 0, NULL);

ADMIN FLUSH_TABLE('prefilter_last_row_null');

INSERT INTO prefilter_last_row_null(host, ts, cpu) VALUES ('host1', 0, 10.0);

ADMIN FLUSH_TABLE('prefilter_last_row_null');

SELECT COUNT(*) FROM prefilter_last_row_null WHERE cpu IS NULL;

ALTER TABLE prefilter_last_row_null ADD COLUMN memory DOUBLE NULL;

INSERT INTO prefilter_last_row_null(host, ts, cpu, memory) VALUES ('host1', 0, 10.0, 20.0);

ADMIN FLUSH_TABLE('prefilter_last_row_null');

SELECT COUNT(*) FROM prefilter_last_row_null WHERE memory IS NULL;

DROP TABLE prefilter_last_row_null;
