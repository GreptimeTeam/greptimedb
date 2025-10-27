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
