CREATE TABLE narthex_physical (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    cpu DOUBLE
) ENGINE=metric WITH (physical_metric_table='true', sst_format='narthex');

CREATE TABLE narthex_cpu (
    ts TIMESTAMP TIME INDEX,
    host STRING PRIMARY KEY,
    cpu DOUBLE
) ENGINE=metric WITH (on_physical_table='narthex_physical');

INSERT INTO narthex_cpu (ts, host, cpu) VALUES (1000, 'a', 1.5), (2000, 'b', 2.5);

SELECT host, cpu FROM narthex_cpu ORDER BY host;

ADMIN FLUSH_TABLE('narthex_physical');

INSERT INTO narthex_cpu (ts, host, cpu) VALUES (3000, 'a', 3.0);

SELECT host, SUM(cpu) FROM narthex_cpu GROUP BY host ORDER BY host;

SELECT ts, host, cpu FROM narthex_cpu ORDER BY ts;

SELECT ts, host, cpu FROM narthex_cpu ORDER BY ts DESC;

DROP TABLE narthex_cpu;

DROP TABLE narthex_physical;
