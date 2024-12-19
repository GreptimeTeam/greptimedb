SELECT vec_to_string(vec_scalar_add(1.0, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_add(-1.0, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_add(1.0, parse_vec('[1.0, 2.0]')));

SELECT vec_to_string(vec_scalar_add(-1.0, parse_vec('[1.0, 2.0]')));

SELECT vec_to_string(vec_scalar_add(1, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_add(-1, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_mul(1.0, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_mul(-0.5, '[2.0, 4.0]'));

SELECT vec_to_string(vec_scalar_mul(1.0, parse_vec('[1.0, 2.0]')));

SELECT vec_to_string(vec_scalar_mul(-0.5, parse_vec('[2.0, 4.0]')));

SELECT vec_to_string(vec_scalar_mul(1, '[1.0, 2.0]'));

SELECT vec_to_string(vec_scalar_mul(-0.5, '[2.0, 4.0]'));