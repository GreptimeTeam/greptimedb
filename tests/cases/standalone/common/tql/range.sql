-- Test sum(rate()) function combinations
CREATE TABLE metrics (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING,
    `service` STRING,
    PRIMARY KEY (host, service)
);

-- Insert test data with multiple time series
INSERT INTO metrics VALUES
    -- host1, service1
    (0, 10, 'host1', 'service1'),
    (30000, 15, 'host1', 'service1'),
    (60000, 20, 'host1', 'service1'),
    (90000, 25, 'host1', 'service1'),
    (120000, 30, 'host1', 'service1'),
    (150000, 35, 'host1', 'service1'),
    (180000, 40, 'host1', 'service1'),
    (210000, 45, 'host1', 'service1'),
    (240000, 50, 'host1', 'service1'),
    -- host1, service2
    (0, 5, 'host1', 'service2'),
    (30000, 10, 'host1', 'service2'),
    (60000, 15, 'host1', 'service2'),
    (90000, 20, 'host1', 'service2'),
    (120000, 25, 'host1', 'service2'),
    (150000, 30, 'host1', 'service2'),
    (180000, 35, 'host1', 'service2'),
    (210000, 40, 'host1', 'service2'),
    (240000, 45, 'host1', 'service2'),
    -- host2, service1
    (0, 8, 'host2', 'service1'),
    (30000, 13, 'host2', 'service1'),
    (60000, 18, 'host2', 'service1'),
    (90000, 23, 'host2', 'service1'),
    (120000, 28, 'host2', 'service1'),
    (150000, 33, 'host2', 'service1'),
    (180000, 38, 'host2', 'service1'),
    (210000, 43, 'host2', 'service1'),
    (240000, 48, 'host2', 'service1');

-- Test basic sum(rate()) - sum rate across all series
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics[1m]));

-- Test sum(rate()) with grouping by host
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum by(host) (rate(metrics[1m]));

-- Test sum(rate()) with grouping by service
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum by(service) (rate(metrics[1m]));

-- Test sum(rate()) with label filtering
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics{host="host1"}[1m]));

-- Test sum(rate()) with multiple label filters
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics{host="host1", service="service1"}[1m]));

-- Test sum(rate()) with regex label matching
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics{host=~"host.*"}[1m]));

-- Test sum(rate()) with different time ranges
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics[31s]));

-- Test sum(rate()) with longer evaluation window
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 240, '60s') sum(rate(metrics[1m]));

-- Test sum(rate()) combined with arithmetic operations
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum(rate(metrics[1m])) * 100;

-- Test sum(rate()) with grouping and arithmetic
-- SQLNESS SORT_RESULT 2 1
TQL EVAL (60, 180, '60s') sum by(host) (rate(metrics[1m])) * 60;

-- Test querying non-existent table
TQL EVAL (60, 180, '60s') sum(rate(non_existent_table[1m]));

-- Test querying non-existent label
TQL EVAL (60, 180, '60s') sum(rate(metrics{non_existent_label="value"}[1m]));

-- Test querying non-existent label value
TQL EVAL (60, 180, '60s') sum(rate(metrics{host="non_existent_host"}[1m]));

-- Test querying multiple non-existent labels
TQL EVAL (60, 180, '60s') sum(rate(metrics{non_existent_label1="value1", non_existent_label2="value2"}[1m]));

-- Test querying mix of existing and non-existent labels
TQL EVAL (60, 180, '60s') sum(rate(metrics{host="host1", non_existent_label="value"}[1m]));

-- Test querying non-existent table with non-existent labels
TQL EVAL (60, 180, '60s') sum(rate(non_existent_table{non_existent_label="value"}[1m]));

-- Test querying non-existent table with multiple non-existent labels
TQL EVAL (60, 180, '60s') sum(rate(non_existent_table{label1="value1", label2="value2"}[1m]));

DROP TABLE metrics;
