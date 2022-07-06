use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::compute::cast::primitive_to_primitive;
use arrow::datatypes::DataType::Float64;
use datatypes::data_type::DataType;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::value::Value;
use datatypes::vectors::Float64Vector;
use datatypes::vectors::PrimitiveVector;
use datatypes::vectors::Vector;
use datatypes::vectors::VectorRef;
use datatypes::with_match_primitive_type_id;
use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "the length of the len1  {}  dost not match the length of the len2  {}",
        len1,
        len2,
    ))]
    LenNotEquals { len1: usize, len2: usize },

    #[snafu(display("the sample is empty"))]
    SampleEmpty { len: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

fn linear_search(x: Value, xp: &PrimitiveVector<f64>) -> usize {
    for i in 0..xp.len() {
        if x < xp.get(i) {
            return i - 1;
        }
    }
    xp.len()
}

fn binary_search(key: Value, xp: &PrimitiveVector<f64>) -> usize {
    let mut left = 0;
    let mut right = xp.len();
    /* If len <= 4 use linear search. */
    if xp.len() <= 4 {
        return linear_search(key, xp);
    }
    /* find index by bisection */
    while left < right {
        let mid = left + ((right - left) >> 1);
        if key >= xp.get(mid) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left - 1
}

fn concrete_type_to_primitive_vector(arg: &VectorRef) -> Result<PrimitiveVector<f64>> {
    with_match_primitive_type_id!(arg.data_type().logical_type_id(), |$S| {
        let tmp=arg.to_arrow_array();
        let from = tmp.as_any().downcast_ref::<PrimitiveArray<$S>>().expect("cast failed");
        let array = primitive_to_primitive(from, &Float64);
        Ok(PrimitiveVector::new(array))
    },{
        unreachable!()
    })
}

/// https://github.com/numpy/numpy/blob/b101756ac02e390d605b2febcded30a1da50cc2c/numpy/core/src/multiarray/compiled_base.c
// begin 492
pub fn interp(args: &[VectorRef]) -> Result<VectorRef> {
    let mut left = None;
    let mut right = None;

    let x = concrete_type_to_primitive_vector(&args[0])?;
    let xp = concrete_type_to_primitive_vector(&args[1])?;
    let fp = concrete_type_to_primitive_vector(&args[2])?;

    if args.len() == 5 {
        left = concrete_type_to_primitive_vector(&args[3])
            .unwrap()
            .get_data(0);
        right = concrete_type_to_primitive_vector(&args[4])
            .unwrap()
            .get_data(0);
    }

    println!("{:?},{:?}", left, right);
    // 想办法确保args.eln()==3!!args.len()==5
    // ensure!(
    //     args.len() ==3||args.len()==5,
    //     LenNotEqualsSnafu {
    //         expect: col.len(),
    //         given: vector.len(),
    //     }
    // );

    // ensure!(

    // )

    ensure!(x.len() != 0, SampleEmptySnafu { len: x.len() });

    ensure!(
        xp.len() == fp.len(),
        LenNotEqualsSnafu {
            len1: xp.len(),
            len2: fp.len(),
        }
    );

    /* Get left and right fill values. */
    let left = match left {
        Some(left) => Some(left),
        _ => fp.get_data(0),
    };

    let right = match right {
        Some(right) => Some(right),
        _ => fp.get_data(fp.len() - 1),
    };

    let res;
    if xp.len() == 1 {
        res = x
            .iter_data()
            .map(|x| {
                if Value::from(x) < xp.get(0) {
                    left
                } else if Value::from(x) > xp.get(xp.len() - 1) {
                    right
                } else {
                    fp.get_data(0)
                }
            })
            .collect::<Float64Vector>();
    } else {
        let mut j = 0;
        /* only pre-calculate slopes if there are relatively few of them. */
        let mut slopes: Option<Vec<_>> = None;
        if x.len() >= xp.len() {
            let mut slopes_tmp = Vec::with_capacity(xp.len() - 1);
            for i in 0..xp.len() - 1 {
                let slope = match (
                    fp.get_data(i + 1),
                    fp.get_data(i),
                    xp.get_data(i + 1),
                    xp.get_data(i),
                ) {
                    (Some(fp1), Some(fp2), Some(xp1), Some(xp2)) => Some((fp1 - fp2) / (xp1 - xp2)),
                    _ => None,
                };
                slopes_tmp.push(slope);
            }
            slopes = Some(slopes_tmp);
        }
        res = x
            .iter_data()
            .map(|x| match x {
                Some(xi) => {
                    if Value::from(xi) > xp.get(xp.len() - 1) {
                        right
                    } else if Value::from(xi) < xp.get(0) {
                        left
                    } else {
                        j = binary_search(Value::from(xi), &xp);
                        if j == xp.len() - 1 || xp.get(j) == Value::from(xi) {
                            fp.get_data(j)
                        } else {
                            let slope = match &slopes {
                                Some(slopes) => slopes[j],
                                _ => match (
                                    fp.get_data(j + 1),
                                    fp.get_data(j),
                                    xp.get_data(j + 1),
                                    xp.get_data(j),
                                ) {
                                    (Some(fp1), Some(fp2), Some(xp1), Some(xp2)) => {
                                        Some((fp1 - fp2) / (xp1 - xp2))
                                    }
                                    _ => None,
                                },
                            };

                            /* If we get nan in one direction, try the other */
                            let ans = match (slope, xp.get_data(j), fp.get_data(j)) {
                                (Some(slope), Some(xp), Some(fp)) => Some(slope * (xi - xp) + fp),
                                _ => None,
                            };

                            let ans = match ans {
                                Some(ans) => Some(ans),
                                _ => match (slope, xp.get_data(j + 1), fp.get_data(j + 1)) {
                                    (Some(slope), Some(xp), Some(fp)) => {
                                        Some(slope * (xi - xp) + fp)
                                    }
                                    _ => None,
                                },
                            };
                            let ans = match ans {
                                Some(ans) => Some(ans),
                                _ => {
                                    if fp.get_data(j) == fp.get_data(j + 1) {
                                        fp.get_data(j)
                                    } else {
                                        None
                                    }
                                }
                            };
                            ans
                        }
                    }
                }
                _ => None,
            })
            .collect::<Float64Vector>();
    }
    Ok(Arc::new(res) as _)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{Int32Vector, Int64Vector};

    use super::*;
    #[test]
    fn test_interp_function() {
        // x xp fp
        let x = 2.5;
        let xp = vec![1i32, 2i32, 3i32];
        let fp = vec![3i64, 2i64, 0i64];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(vec![x])),
            Arc::new(Int32Vector::from_vec(xp.clone())),
            Arc::new(Int64Vector::from_vec(fp.clone())),
        ];
        let vector = interp(&args).unwrap();
        println!("successful interpret");
        assert_eq!(vector.len(), 1);

        assert!(matches!(vector.get(0), Value::Float64(v) if v==1.0));

        let x = vec![0.0, 1.0, 1.5, 3.2];
        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(x)),
            Arc::new(Int32Vector::from_vec(xp)),
            Arc::new(Int64Vector::from_vec(fp)),
        ];
        let vector = interp(&args).unwrap();
        println!("successful interpret");
        assert_eq!(4, vector.len());
        let res = vec![3.0, 3.0, 2.5, 0.0];
        for (i, item) in res.iter().enumerate().take(vector.len()) {
            assert!(matches!(vector.get(i),Value::Float64(v) if v==*item));
        }
    }
}
