SELECT vec_cos_distance('[1.0, 2.0]', '[0.0, 0.0]');

SELECT vec_cos_distance(parse_vec('[1.0, 2.0]'), '[0.0, 0.0]');

SELECT vec_cos_distance('[1.0, 2.0]', parse_vec('[0.0, 0.0]'));

SELECT vec_cos_distance(parse_vec('[1.0, 2.0]'), parse_vec('[0.0, 0.0]'));

SELECT vec_l2sq_distance('[1.0, 2.0]', '[0.0, 0.0]');

SELECT vec_l2sq_distance(parse_vec('[1.0, 2.0]'), '[0.0, 0.0]');

SELECT vec_l2sq_distance('[1.0, 2.0]', parse_vec('[0.0, 0.0]'));

SELECT vec_l2sq_distance(parse_vec('[1.0, 2.0]'), parse_vec('[0.0, 0.0]'));

SELECT vec_dot_product('[1.0, 2.0]', '[0.0, 0.0]');

SELECT vec_dot_product(parse_vec('[1.0, 2.0]'), '[0.0, 0.0]');

SELECT vec_dot_product('[1.0, 2.0]', parse_vec('[0.0, 0.0]'));

SELECT vec_dot_product(parse_vec('[1.0, 2.0]'), parse_vec('[0.0, 0.0]'));
