CREATE TABLE okx_model_param_mvr_1_late_orig (
    for_ccy STRING,
    dom_ccy STRING,
    expiry STRING,
    "type" STRING,
    ts TIMESTAMP(3) NOT NULL,
    "value" DOUBLE,
    TIME INDEX(ts),
    PRIMARY KEY(for_ccy, dom_ccy, expiry, "type")
);

CREATE FLOW okx_model_param_mvr_orig_flow
SINK TO okx_model_param_mvr_orig_sink
EVAL INTERVAL '1s'
AS
WITH
m1 AS (
    SELECT
        for_ccy,
        dom_ccy,
        expiry,
        "type"                            AS param_type,
        date_bin(INTERVAL '1 second', ts) AS m,
        last_value("value" ORDER BY ts)   AS "value"
    FROM okx_model_param_mvr_1_late_orig
    WHERE ts >  date_trunc('second', now()) - INTERVAL '1 minute'
      AND ts <  date_trunc('second', now())
      AND "type" = 'alpha'
    GROUP BY for_ccy, dom_ccy, expiry, "type", date_bin(INTERVAL '1 second', ts)
),
rolling AS (
    SELECT
        for_ccy, dom_ccy, expiry, param_type, m, "value",
        avg("value")         OVER w AS roll_avg,
        stddev_samp("value") OVER w AS roll_std
    FROM m1
    WINDOW w AS (
        PARTITION BY for_ccy, dom_ccy, expiry, param_type
        ORDER BY m
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
),
ref AS (
    SELECT
        for_ccy, dom_ccy, expiry, param_type, m, "value", roll_avg, roll_std,
        CASE m
            WHEN date_trunc('second', now()) - INTERVAL '1 second' THEN 't'
            WHEN date_trunc('second', now()) - INTERVAL '2 seconds' THEN 'b'
            WHEN date_trunc('second', now()) - INTERVAL '4 seconds' THEN 'c'
        END AS slot
    FROM rolling
    WHERE m IN (
        date_trunc('second', now()) - INTERVAL '1 second',
        date_trunc('second', now()) - INTERVAL '2 seconds',
        date_trunc('second', now()) - INTERVAL '4 seconds'
    )
)
SELECT
    t.for_ccy,
    t.dom_ccy,
    t.expiry,
    t.param_type,
    'modelParamMVR'                                             AS measure_name,
    t.m                                                         AS ts,
    t.roll_avg                                                  AS "avg",
    CASE WHEN t.roll_std IS NOT NULL THEN t.roll_std ELSE 0 END AS "stddev_samp",
    CASE
        WHEN b.roll_avg IS NULL AND b.roll_std IS NULL THEN '1'
        WHEN c.roll_avg IS NULL AND c.roll_std IS NULL THEN '1'
        WHEN t."value" <= b.roll_avg + 3 * b.roll_std
         AND t."value" >= b.roll_avg - 3 * b.roll_std THEN '1'
        ELSE '0'
    END                                                         AS "status",
    now()                                                       AS create_time
FROM      (SELECT * FROM ref WHERE slot = 't') t
LEFT JOIN (SELECT * FROM ref WHERE slot = 'b') b
       ON t.for_ccy = b.for_ccy AND t.dom_ccy = b.dom_ccy
      AND t.expiry  = b.expiry  AND t.param_type = b.param_type
LEFT JOIN (SELECT * FROM ref WHERE slot = 'c') c
       ON t.for_ccy = c.for_ccy AND t.dom_ccy = c.dom_ccy
      AND t.expiry  = c.expiry  AND t.param_type = c.param_type;

INSERT INTO okx_model_param_mvr_1_late_orig VALUES
    ('early_1', 'USD', '202606', 'alpha', date_trunc('second', now()), 1.0),
    ('early_2', 'USD', '202606', 'alpha', date_trunc('second', now()), 2.0),
    ('early_3', 'USD', '202606', 'alpha', date_trunc('second', now()), 3.0),
    ('early_4', 'USD', '202606', 'alpha', date_trunc('second', now()), 4.0);

-- SQLNESS SLEEP 3s
INSERT INTO okx_model_param_mvr_1_late_orig
SELECT 'late_1', 'USD', '202606', 'alpha', min(ts), 10.0 FROM okx_model_param_mvr_1_late_orig
UNION ALL
SELECT 'late_2', 'USD', '202606', 'alpha', min(ts), 11.0 FROM okx_model_param_mvr_1_late_orig;

-- SQLNESS SLEEP 4s
SELECT
    'single-target' AS scenario,
    count(DISTINCT for_ccy) AS sink_groups,
    bool_and(create_time = date_trunc('second', create_time)) AS all_create_time_at_second_boundary,
    bool_and(ts = create_time - INTERVAL '1 second') AS all_ts_match_single_target_schedule
FROM okx_model_param_mvr_orig_sink
WHERE ts = (SELECT min(ts) FROM okx_model_param_mvr_1_late_orig)
  AND param_type = 'alpha';

DROP FLOW okx_model_param_mvr_orig_flow;
DROP TABLE okx_model_param_mvr_orig_sink;
DROP TABLE okx_model_param_mvr_1_late_orig;

CREATE TABLE okx_model_param_mvr_1_late_overlap (
    for_ccy STRING,
    dom_ccy STRING,
    expiry STRING,
    "type" STRING,
    ts TIMESTAMP(3) NOT NULL,
    "value" DOUBLE,
    TIME INDEX(ts),
    PRIMARY KEY(for_ccy, dom_ccy, expiry, "type")
);

CREATE FLOW okx_model_param_mvr_overlap_flow
SINK TO okx_model_param_mvr_overlap_sink
EVAL INTERVAL '1s'
AS
WITH
target_offsets(delta) AS (
    VALUES
        (INTERVAL '1 second'),
        (INTERVAL '2 seconds'),
        (INTERVAL '3 seconds'),
        (INTERVAL '4 seconds'),
        (INTERVAL '5 seconds'),
        (INTERVAL '6 seconds'),
        (INTERVAL '7 seconds'),
        (INTERVAL '8 seconds'),
        (INTERVAL '9 seconds'),
        (INTERVAL '10 seconds')
),
target_seconds AS (
    SELECT date_trunc('second', now()) - delta AS t_m
    FROM target_offsets
),
m1 AS (
    SELECT
        for_ccy,
        dom_ccy,
        expiry,
        "type"                            AS param_type,
        date_bin(INTERVAL '1 second', ts) AS m,
        last_value("value" ORDER BY ts)   AS "value"
    FROM okx_model_param_mvr_1_late_overlap
    WHERE ts >  date_trunc('second', now()) - INTERVAL '1 minute'
      AND ts <  date_trunc('second', now())
      AND "type" = 'alpha'
    GROUP BY for_ccy, dom_ccy, expiry, "type", date_bin(INTERVAL '1 second', ts)
),
rolling AS (
    SELECT
        for_ccy, dom_ccy, expiry, param_type, m, "value",
        avg("value")         OVER w AS roll_avg,
        stddev_samp("value") OVER w AS roll_std
    FROM m1
    WINDOW w AS (
        PARTITION BY for_ccy, dom_ccy, expiry, param_type
        ORDER BY m
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )
),
ref AS (
    SELECT
        ts_target.t_m,
        r.for_ccy,
        r.dom_ccy,
        r.expiry,
        r.param_type,
        r.m,
        r."value",
        r.roll_avg,
        r.roll_std,
        CASE
            WHEN r.m = ts_target.t_m THEN 't'
            WHEN r.m = ts_target.t_m - INTERVAL '1 second' THEN 'b'
            WHEN r.m = ts_target.t_m - INTERVAL '3 seconds' THEN 'c'
        END AS slot
    FROM rolling r
    JOIN target_seconds ts_target
      ON r.m = ts_target.t_m
      OR r.m = ts_target.t_m - INTERVAL '1 second'
      OR r.m = ts_target.t_m - INTERVAL '3 seconds'
)
SELECT
    t.for_ccy,
    t.dom_ccy,
    t.expiry,
    t.param_type,
    'modelParamMVR'                                             AS measure_name,
    t.m                                                         AS ts,
    t.roll_avg                                                  AS "avg",
    CASE WHEN t.roll_std IS NOT NULL THEN t.roll_std ELSE 0 END AS "stddev_samp",
    CASE
        WHEN b.roll_avg IS NULL AND b.roll_std IS NULL THEN '1'
        WHEN c.roll_avg IS NULL AND c.roll_std IS NULL THEN '1'
        WHEN t."value" <= b.roll_avg + 3 * b.roll_std
         AND t."value" >= b.roll_avg - 3 * b.roll_std THEN '1'
        ELSE '0'
    END                                                         AS "status",
    now()                                                       AS create_time
FROM      (SELECT * FROM ref WHERE slot = 't') t
LEFT JOIN (SELECT * FROM ref WHERE slot = 'b') b
       ON t.t_m = b.t_m
      AND t.for_ccy = b.for_ccy AND t.dom_ccy = b.dom_ccy
      AND t.expiry  = b.expiry  AND t.param_type = b.param_type
LEFT JOIN (SELECT * FROM ref WHERE slot = 'c') c
       ON t.t_m = c.t_m
      AND t.for_ccy = c.for_ccy AND t.dom_ccy = c.dom_ccy
      AND t.expiry  = c.expiry  AND t.param_type = c.param_type;

INSERT INTO okx_model_param_mvr_1_late_overlap VALUES
    ('early_1', 'USD', '202606', 'alpha', date_trunc('second', now()), 1.0),
    ('early_2', 'USD', '202606', 'alpha', date_trunc('second', now()), 2.0),
    ('early_3', 'USD', '202606', 'alpha', date_trunc('second', now()), 3.0),
    ('early_4', 'USD', '202606', 'alpha', date_trunc('second', now()), 4.0);

-- SQLNESS SLEEP 3s
INSERT INTO okx_model_param_mvr_1_late_overlap
SELECT 'late_1', 'USD', '202606', 'alpha', min(ts), 10.0 FROM okx_model_param_mvr_1_late_overlap
UNION ALL
SELECT 'late_2', 'USD', '202606', 'alpha', min(ts), 11.0 FROM okx_model_param_mvr_1_late_overlap;

-- SQLNESS SLEEP 4s
SELECT
    'overlap' AS scenario,
    count(DISTINCT for_ccy) AS sink_groups,
    bool_and(create_time = date_trunc('second', create_time)) AS all_create_time_at_second_boundary,
    bool_and(ts < create_time) AS all_ts_before_create_time,
    bool_and(ts >= create_time - INTERVAL '10 seconds') AS all_ts_within_overlap_window
FROM okx_model_param_mvr_overlap_sink
WHERE ts = (SELECT min(ts) FROM okx_model_param_mvr_1_late_overlap)
  AND param_type = 'alpha';

DROP FLOW okx_model_param_mvr_overlap_flow;
DROP TABLE okx_model_param_mvr_overlap_sink;
DROP TABLE okx_model_param_mvr_1_late_overlap;
