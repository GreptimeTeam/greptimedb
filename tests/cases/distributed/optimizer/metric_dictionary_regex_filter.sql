-- Exercise dictionary-encoded metric labels across physical-table partitions.
CREATE TABLE metric_dictionary_regex_filter_physical (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING,
    val DOUBLE,
    PRIMARY KEY(host)
)
PARTITION ON COLUMNS (host) (
    host < 'api-02',
    host >= 'api-02' AND host < 'db',
    host >= 'db'
)
ENGINE = metric
WITH (
    physical_metric_table = "true"
);

CREATE TABLE metric_dictionary_regex_filter (
    ts TIMESTAMP(3) TIME INDEX,
    host STRING PRIMARY KEY,
    val DOUBLE
)
ENGINE = metric
WITH (
    on_physical_table = "metric_dictionary_regex_filter_physical"
);

INSERT INTO metric_dictionary_regex_filter (ts, host, val) VALUES
    (1000, 'api-01', 1.0),
    (2000, 'api-02', 2.0),
    (3000, 'db-01', 3.0);

ADMIN FLUSH_TABLE('metric_dictionary_regex_filter_physical');

-- SQLNESS SORT_RESULT 3 1
TQL EVAL (2, 2, '1s') metric_dictionary_regex_filter{host=~"api-.*"};

SELECT host, ts, val
FROM metric_dictionary_regex_filter
WHERE host ~ '^api-.*$'
ORDER BY host, ts;

SELECT host, ts, val
FROM metric_dictionary_regex_filter
WHERE host !~ '^api-.*$'
ORDER BY host, ts;

DROP TABLE metric_dictionary_regex_filter;
DROP TABLE metric_dictionary_regex_filter_physical;
