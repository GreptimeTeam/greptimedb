use std::sync::Arc;

use common_query::error::Result;
use datatypes::prelude::ScalarVector;
use datatypes::value::Value;
use datatypes::vectors::Float64Vector;
use datatypes::vectors::Vector;
use datatypes::vectors::VectorRef;
use num_traits::cast::AsPrimitive;

pub fn interp(args: &[VectorRef]) -> Result<VectorRef> {
    assert_eq!(args.len(), 3);
    assert_eq!(args[1].len(), args[2].len());

    let x = args[0]
        .as_any()
        .downcast_ref::<Float64Vector>()
        .expect("cast failed");

    let xp = args[1]
        .as_any()
        .downcast_ref::<Float64Vector>()
        .expect("cast failed");

    let fp = args[2]
        .as_any()
        .downcast_ref::<Float64Vector>()
        .expect("cast failed");

    assert!(x.len()>=1);
    
    let x: Vec<_> = x
        .iter_data()
        .collect::<Vec<Option<f64>>>()
        .into_iter()
        .flatten()
        .collect();

    let xp: Vec<_> = xp
        .iter_data()
        .collect::<Vec<Option<f64>>>()
        .into_iter()
        .flatten()
        .collect();

    let fp: Vec<_> = fp
        .iter_data()
        .collect::<Vec<Option<f64>>>()
        .into_iter()
        .flatten()
        .collect();

    let mut slopes = Vec::new();
    if x.len() >= xp.len() {
        for i in 0..xp.len() - 1 {
            slopes.push((fp[i + 1] - fp[i]) / (xp[i + 1] - xp[i]));
        }
    }

    let res: Vec<f64> = x
        .iter()
        .map(|xi| {
            if xi <= &xp[0] {
                fp[0]
            } else if xi >= &xp[xp.len() - 1] {
                fp[xp.len() - 1]
            } else {
                let j = binary_search(*xi, Arc::new(Float64Vector::from_vec(xp.clone())));
                if x.len() >= xp.len() {
                    let slope = slopes[j];
                    slope * (xi - xp[j]) + fp[j]
                } else {
                    let slope = (fp[j + 1] - fp[j]) / (xp[j + 1] - xp[j]);
                    slope * (xi - xp[j]) + fp[j]
                }
            }
        })
        .collect();

    let res = Arc::new(Float64Vector::from_vec(res));
    Ok(res as _)
}

fn binary_search<S>(x: S, xp: VectorRef) -> usize
where
    S: AsPrimitive<f64>,
{
    let mut left = 0;
    let mut right = xp.len() - 1;
    while left < right {
        let mid = left + ((right - left + 1) >> 1);
        if xp.get(mid) <= Value::from(x.as_()) {
            left = mid;
        } else if xp.get(mid) > Value::from(x.as_()) {
            right = mid - 1;
        }
    }
    left
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_binary_search_function() {
        let x = vec![1i32, 2i32, 3i32, 4i32, 5i32, 6i32, 7i32];
        let xp = vec![1.0, 3.0, 5.0, 7.0, 9.0];
        let xp = Arc::new(Float64Vector::from_vec(xp));
        let expect = Arc::new(Float64Vector::from_vec(vec![
            1.0, 1.0, 3.0, 3.0, 5.0, 5.0, 7.0,
        ]));

        let mut res = Vec::new();
        for &i in x.iter() {
            let pos = binary_search(i, xp.clone());
            res.push(xp.get(pos));
        }
        for (i, item) in res.iter().enumerate() {
            assert_eq!(*item, expect.get(i));
        }
    }
    #[test]
    fn test_interp_function() {
        let x = 2.5;
        let xp = vec![1.0, 2.0, 3.0];
        let fp = vec![3.0, 2.0, 0.0];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(vec![x])),
            Arc::new(Float64Vector::from_vec(xp.clone())),
            Arc::new(Float64Vector::from_vec(fp.clone())),
        ];
        let vector = interp(&args).unwrap();
        assert_eq!(vector.len(), 1);

        assert!(matches!(vector.get(0), Value::Float64(v) if v==1.0));

        let x = vec![0.0, 1.0, 1.5, 3.2];
        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(x)),
            Arc::new(Float64Vector::from_vec(xp)),
            Arc::new(Float64Vector::from_vec(fp)),
        ];

        let vector = interp(&args).unwrap();
        assert_eq!(4, vector.len());
        let res = vec![3.0, 3.0, 2.5, 0.0];
        for (i, item) in res.iter().enumerate().take(vector.len()) {
            assert!(matches!(vector.get(i),Value::Float64(v) if v==*item));
        }
    }
}
