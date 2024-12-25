SELECT vec_to_string(parse_vec('[1.0, 2.0]'));

SELECT vec_to_string(parse_vec('[1.0, 2.0, 3.0]'));

SELECT vec_to_string(parse_vec('[]'));

SELECT vec_to_string(vec_mul('[1.0, 2.0]', '[3.0, 4.0]'));

SELECT vec_to_string(vec_mul(parse_vec('[1.0, 2.0]'), '[3.0, 4.0]'));

SELECT vec_to_string(vec_mul('[1.0, 2.0]', parse_vec('[3.0, 4.0]')));