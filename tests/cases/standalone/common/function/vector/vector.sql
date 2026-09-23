SELECT vec_to_string(parse_vec('[1.0, 2.0]'));

SELECT vec_to_string(parse_vec('[1.0, 2.0, 3.0]'));

SELECT vec_to_string(parse_vec('[]'));

SELECT vec_to_string(vec_add('[1.0, 2.0]', '[3.0, 4.0]'));

SELECT vec_to_string(vec_add(parse_vec('[1.0, 2.0]'), '[3.0, 4.0]'));

SELECT vec_to_string(vec_add('[1.0, 2.0]', parse_vec('[3.0, 4.0]')));

SELECT vec_to_string(vec_mul('[1.0, 2.0]', '[3.0, 4.0]'));

SELECT vec_to_string(vec_mul(parse_vec('[1.0, 2.0]'), '[3.0, 4.0]'));

SELECT vec_to_string(vec_mul('[1.0, 2.0]', parse_vec('[3.0, 4.0]')));

SELECT vec_to_string(vec_sub('[1.0, 1.0]', '[1.0, 2.0]'));

SELECT vec_to_string(vec_sub('[-1.0, -1.0]', '[1.0, 2.0]'));

SELECT vec_to_string(vec_sub('[1.0, 1.0]', parse_vec('[1.0, 2.0]')));

SELECT vec_to_string(vec_sub('[-1.0, -1.0]', parse_vec('[1.0, 2.0]')));

SELECT vec_to_string(vec_sub(parse_vec('[1.0, 1.0]'), '[1.0, 2.0]'));

SELECT vec_to_string(vec_sub(parse_vec('[-1.0, -1.0]'), '[1.0, 2.0]'));

SELECT vec_elem_sum('[1.0, 2.0, 3.0]');

SELECT vec_elem_sum('[-1.0, -2.0, -3.0]');

SELECT vec_elem_sum(parse_vec('[1.0, 2.0, 3.0]'));

SELECT vec_elem_sum(parse_vec('[-1.0, -2.0, -3.0]'));

SELECT vec_elem_avg('[1.0, 2.0, 3.0]');

SELECT vec_elem_avg('[-1.0, -2.0, -3.0]');

SELECT vec_elem_avg(parse_vec('[1.0, 2.0, 3.0]'));

SELECT vec_elem_avg(parse_vec('[-1.0, -2.0, -3.0]'));

SELECT vec_to_string(vec_div('[1.0, 2.0]', '[3.0, 4.0]'));

SELECT vec_to_string(vec_div(parse_vec('[1.0, 2.0]'), '[3.0, 4.0]'));

SELECT vec_to_string(vec_div('[1.0, 2.0]', parse_vec('[3.0, 4.0]')));

SELECT vec_to_string(vec_div('[1.0, -2.0]', parse_vec('[0.0, 0.0]')));

SELECT vec_elem_product('[1.0, 2.0, 3.0, 4.0]');

SELECT vec_elem_product('[-1.0, -2.0, -3.0, 4.0]');

SELECT vec_elem_product(parse_vec('[1.0, 2.0, 3.0, 4.0]'));

SELECT vec_elem_product(parse_vec('[-1.0, -2.0, -3.0, 4.0]'));

SELECT vec_to_string(vec_norm('[0.0, 2.0, 3.0]'));

SELECT vec_to_string(vec_norm('[1.0, 2.0, 3.0]'));

SELECT vec_to_string(vec_norm('[7.0, 8.0, 9.0]'));

SELECT vec_to_string(vec_norm('[7.0, -8.0, 9.0]'));

SELECT vec_to_string(vec_norm(parse_vec('[7.0, -8.0, 9.0]')));

SELECT vec_to_string(vec_sum(v))
FROM (
    SELECT '[1.0, 2.0, 3.0]' AS v
    UNION ALL
    SELECT '[-1.0, -2.0, -3.0]' AS v
    UNION ALL
    SELECT '[4.0, 5.0, 6.0]' AS v
);

SELECT vec_to_string(vec_avg(v))
FROM (
    SELECT '[1.0, 2.0, 3.0]' AS v
    UNION ALL
    SELECT '[10.0, 11.0, 12.0]' AS v
    UNION ALL
    SELECT '[4.0, 5.0, 6.0]' AS v
);

SELECT vec_to_string(vec_product(v))
FROM (
    SELECT '[1.0, 2.0, 3.0]' AS v
    UNION ALL
    SELECT '[-1.0, -2.0, -3.0]' AS v
    UNION ALL
    SELECT '[4.0, 5.0, 6.0]' AS v
);

SELECT vec_dim('[7.0, 8.0, 9.0, 10.0]');

SELECT v, vec_dim(v)
FROM (
         SELECT '[1.0, 2.0, 3.0]' AS v
         UNION ALL
         SELECT '[-1.0]' AS v
         UNION ALL
         SELECT '[4.0, 5.0, 6.0]' AS v
     ) Order By vec_dim(v) ASC;

SELECT v, vec_dim(v)
FROM (
         SELECT '[1.0, 2.0, 3.0]' AS v
         UNION ALL
         SELECT '[-1.0]' AS v
         UNION ALL
         SELECT '[7.0, 8.0, 9.0, 10.0]' AS v
     ) Order By vec_dim(v) ASC;

SELECT vec_kth_elem('[1.0, 2.0, 3.0]', 2);

SELECT v, vec_kth_elem(v, 0) AS first_elem
FROM (
         SELECT '[1.0, 2.0, 3.0]' AS v
         UNION ALL
         SELECT '[4.0, 5.0, 6.0, 7.0]' AS v
         UNION ALL
         SELECT '[8.0]' AS v
     )
WHERE vec_kth_elem(v, 0) > 2.0 ORDER BY first_elem;

SELECT vec_to_string(vec_subvector('[1.0,2.0,3.0,4.0,5.0]', 0, 3));

SELECT vec_to_string(vec_subvector('[1.0,2.0,3.0,4.0,5.0]', 5, 5));

SELECT v, vec_to_string(vec_subvector(v, 3, 5))
FROM (
     SELECT '[1.0, 2.0, 3.0, 4.0, 5.0]' AS v
     UNION ALL
     SELECT '[-1.0, -2.0, -3.0, -4.0, -5.0, -6.0]' AS v
     UNION ALL
     SELECT '[4.0, 5.0, 6.0, 10, -8, 100]' AS v
) ORDER BY v;

SELECT vec_to_string(vec_subvector(v, 0, 5))
FROM (
     SELECT '[1.1, 2.2, 3.3, 4.4, 5.5]' AS v
     UNION ALL
     SELECT '[-1.1, -2.1, -3.1, -4.1, -5.1, -6.1]' AS v
     UNION ALL
     SELECT '[4.0, 5.0, 6.0, 10, -8, 100]' AS v
) ORDER BY v;

SELECT h, vec_to_string(vec_sum(v)), vec_to_string(vec_avg(v)), vec_to_string(vec_product(v))
FROM (
    SELECT 'a' AS h, '[1.0, 1.0]' AS v
    UNION ALL
    SELECT 'a' AS h, '[2.0, 2.0]' AS v
    UNION ALL
    SELECT 'b' AS h, '[3.0, 3.0]' AS v
) GROUP BY h ORDER BY h;

-- On partitioned tables the aggregates are split into partial state and merge.
-- Rows only land in two of the three partitions, the third one has an empty state.
CREATE TABLE vector_aggr_partitioned (
    ts TIMESTAMP TIME INDEX,
    k INT,
    g STRING,
    v VECTOR(2),
    PRIMARY KEY(k)
)
PARTITION ON COLUMNS (k) (k < 10, k >= 10 AND k < 20, k >= 20);

INSERT INTO vector_aggr_partitioned VALUES
    (1000, 1, 'a', '[1.0, 1.0]'),
    (2000, 11, 'a', '[2.0, 2.0]'),
    (3000, 2, 'a', '[3.0, 3.0]'),
    (4000, 12, 'b', '[4.0, 4.0]');

SELECT vec_to_string(vec_sum(v)), vec_to_string(vec_avg(v)), vec_to_string(vec_product(v))
FROM vector_aggr_partitioned;

SELECT g, vec_to_string(vec_sum(v)), vec_to_string(vec_avg(v)), vec_to_string(vec_product(v))
FROM vector_aggr_partitioned GROUP BY g ORDER BY g;

-- A NULL vector makes vec_sum and vec_product NULL, vec_avg skips it.
INSERT INTO vector_aggr_partitioned VALUES (5000, 21, 'b', NULL);

SELECT vec_to_string(vec_sum(v)), vec_to_string(vec_avg(v)), vec_to_string(vec_product(v))
FROM vector_aggr_partitioned;

SELECT g, vec_to_string(vec_sum(v)), vec_to_string(vec_avg(v)), vec_to_string(vec_product(v))
FROM vector_aggr_partitioned GROUP BY g ORDER BY g;

DROP TABLE vector_aggr_partitioned;
