-- Infer the canonical native-histogram Struct from a typed Parquet fixture.
-- OTLP endpoint tests cover production ingestion.
CREATE TABLE delta_histogram_source (
    greptime_timestamp TIMESTAMP TIME INDEX,
    histogram_schema INT, zero_threshold DOUBLE, sample_sum DOUBLE, reset_hint INT,
    start_timestamp TIMESTAMP, count_i64 BIGINT, zero_count_i64 BIGINT,
    absent_float DOUBLE
);
INSERT INTO delta_histogram_source VALUES
    (60000, 0, 0.001, 15.0, 3, 0, 10, 0, NULL),
    (120000, 0, 0.001, 30.0, 3, 60000, 20, 0, NULL),
    (180000, 0, 0.001, 22.5, 3, 120000, 15, 0, NULL);

COPY (
    SELECT greptime_timestamp,
        named_struct(
            'schema', histogram_schema,
            'zero_threshold', zero_threshold,
            'sum', sample_sum,
            'reset_hint', reset_hint,
            'start_timestamp', start_timestamp,
            'custom_values', array_slice([sample_sum], 1, 0),
            'positive_span_offsets', [CAST(histogram_schema + 1 AS INT)],
            'positive_span_lengths', [CAST(histogram_schema + 1 AS INT)],
            'negative_span_offsets', array_slice([histogram_schema], 1, 0),
            'negative_span_lengths', array_slice([histogram_schema], 1, 0),
            'count_i64', count_i64,
            'zero_count_i64', zero_count_i64,
            'positive_buckets_i64', [count_i64],
            'negative_buckets_i64', array_slice([count_i64], 1, 0),
            'count_f64', absent_float,
            'zero_count_f64', absent_float,
            'positive_buckets_f64', array_slice([sample_sum], 1, 0),
            'negative_buckets_f64', array_slice([sample_sum], 1, 0)
        ) AS greptime_native_histogram
    FROM delta_histogram_source
) TO 'delta_histogram/data.parquet';

CREATE EXTERNAL TABLE delta_exponential_histogram
WITH (location='delta_histogram/data.parquet', format='parquet');

TQL EVAL (180, 180, '1m') histogram_count(sum_over_time(delta_exponential_histogram[3m]));
TQL EVAL (180, 180, '1m') histogram_sum(sum_over_time(delta_exponential_histogram[3m]));
TQL EVAL (180, 180, '1m') histogram_count(sum_over_time(delta_exponential_histogram[3m]) / 180);
TQL EVAL (180, 180, '1m') histogram_fraction(1, 2, sum_over_time(delta_exponential_histogram[3m]));
-- A single endpoint contributes its whole interval; the left endpoint is excluded.
TQL EVAL (180, 180, '1m') histogram_count(sum_over_time(delta_exponential_histogram[1m]));
TQL EVAL (360, 360, '1m') histogram_count(sum_over_time(delta_exponential_histogram[3m]));

DROP TABLE delta_exponential_histogram;
DROP TABLE delta_histogram_source;
