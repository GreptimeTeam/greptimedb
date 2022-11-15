// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow::array::PrimitiveArray;
use arrow::compute::cast::primitive_to_primitive;
use arrow::datatypes::DataType::Float64;
use datatypes::data_type::DataType;
use datatypes::prelude::ScalarVector;
use datatypes::type_id::LogicalTypeId;
use datatypes::value::Value;
use datatypes::vectors::{Float64Vector, PrimitiveVector, Vector, VectorRef};
use datatypes::{arrow, with_match_primitive_type_id};
use snafu::{ensure, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The length of the args is not enough, expect at least: {}, have: {}",
        expect,
        actual,
    ))]
    ArgsLenNotEnough { expect: usize, actual: usize },

    #[snafu(display("The sample {} is empty", name))]
    SampleEmpty { name: String },

    #[snafu(display(
        "The length of the len1: {} don't match the length of the len2: {}",
        len1,
        len2,
    ))]
    LenNotEquals { len1: usize, len2: usize },
}

pub type Result<T> = std::result::Result<T, Error>;

/* search the biggest number that smaller than x in xp */
fn linear_search_ascending_vector(x: Value, xp: &PrimitiveVector<f64>) -> usize {
    for i in 0..xp.len() {
        if x < xp.get(i) {
            return i - 1;
        }
    }
    xp.len() - 1
}

/* search the biggest number that smaller than x in xp */
fn binary_search_ascending_vector(key: Value, xp: &PrimitiveVector<f64>) -> usize {
    let mut left = 0;
    let mut right = xp.len();
    /* If len <= 4 use linear search. */
    if xp.len() <= 4 {
        return linear_search_ascending_vector(key, xp);
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
        let tmp = arg.to_arrow_array();
        let from = tmp.as_any().downcast_ref::<PrimitiveArray<$S>>().expect("cast failed");
        let array = primitive_to_primitive(from, &Float64);
        Ok(PrimitiveVector::new(array))
    },{
        unreachable!()
    })
}

/// https://github.com/numpy/numpy/blob/b101756ac02e390d605b2febcded30a1da50cc2c/numpy/core/src/multiarray/compiled_base.c#L491
pub fn interp(args: &[VectorRef]) -> Result<VectorRef> {
    let mut left = None;
    let mut right = None;

    ensure!(
        args.len() >= 3,
        ArgsLenNotEnoughSnafu {
            expect: 3_usize,
            actual: args.len()
        }
    );

    let x = concrete_type_to_primitive_vector(&args[0])?;
    let xp = concrete_type_to_primitive_vector(&args[1])?;
    let fp = concrete_type_to_primitive_vector(&args[2])?;

    // make sure the args.len() is 3 or 5
    if args.len() > 3 {
        ensure!(
            args.len() == 5,
            ArgsLenNotEnoughSnafu {
                expect: 5_usize,
                actual: args.len()
            }
        );

        left = concrete_type_to_primitive_vector(&args[3])
            .unwrap()
            .get_data(0);
        right = concrete_type_to_primitive_vector(&args[4])
            .unwrap()
            .get_data(0);
    }

    ensure!(x.len() != 0, SampleEmptySnafu { name: "x" });
    ensure!(xp.len() != 0, SampleEmptySnafu { name: "xp" });
    ensure!(fp.len() != 0, SampleEmptySnafu { name: "fp" });
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
                    (Some(fp1), Some(fp2), Some(xp1), Some(xp2)) => {
                        if xp1 == xp2 {
                            None
                        } else {
                            Some((fp1 - fp2) / (xp1 - xp2))
                        }
                    }
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
                        j = binary_search_ascending_vector(Value::from(xi), &xp);
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
                                        if xp1 == xp2 {
                                            None
                                        } else {
                                            Some((fp1 - fp2) / (xp1 - xp2))
                                        }
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

    use datatypes::prelude::ScalarVectorBuilder;
    use datatypes::vectors::{Int32Vector, Int64Vector, PrimitiveVectorBuilder};

    use super::*;
    #[test]
    fn test_basic_interp() {
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
        assert_eq!(vector.len(), 1);

        assert!(matches!(vector.get(0), Value::Float64(v) if v==1.0));

        let x = vec![0.0, 1.0, 1.5, 3.2];
        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(x)),
            Arc::new(Int32Vector::from_vec(xp)),
            Arc::new(Int64Vector::from_vec(fp)),
        ];
        let vector = interp(&args).unwrap();
        assert_eq!(4, vector.len());
        let res = vec![3.0, 3.0, 2.5, 0.0];
        for (i, item) in res.iter().enumerate().take(vector.len()) {
            assert!(matches!(vector.get(i),Value::Float64(v) if v==*item));
        }
    }

    #[test]
    fn test_left_right() {
        let x = vec![0.0, 1.0, 1.5, 2.0, 3.0, 4.0];
        let xp = vec![1i32, 2i32, 3i32];
        let fp = vec![3i64, 2i64, 0i64];
        let left = vec![-1];
        let right = vec![2];

        let expect = vec![-1.0, 3.0, 2.5, 2.0, 0.0, 2.0];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(x)),
            Arc::new(Int32Vector::from_vec(xp)),
            Arc::new(Int64Vector::from_vec(fp)),
            Arc::new(Int32Vector::from_vec(left)),
            Arc::new(Int32Vector::from_vec(right)),
        ];
        let vector = interp(&args).unwrap();

        for (i, item) in expect.iter().enumerate().take(vector.len()) {
            assert!(matches!(vector.get(i),Value::Float64(v) if v==*item));
        }
    }

    #[test]
    fn test_scalar_interpolation_point() {
        // x=0 output:0
        let x = vec![0];
        let xp = vec![0, 1, 5];
        let fp = vec![0, 1, 5];
        let args: Vec<VectorRef> = vec![
            Arc::new(Int64Vector::from_vec(x.clone())),
            Arc::new(Int64Vector::from_vec(xp.clone())),
            Arc::new(Int64Vector::from_vec(fp.clone())),
        ];
        let vector = interp(&args).unwrap();
        assert!(matches!(vector.get(0), Value::Float64(v) if v==x[0] as f64));

        // x=0.3 output:0.3
        let x = vec![0.3];
        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(x.clone())),
            Arc::new(Int64Vector::from_vec(xp.clone())),
            Arc::new(Int64Vector::from_vec(fp.clone())),
        ];
        let vector = interp(&args).unwrap();
        assert!(matches!(vector.get(0), Value::Float64(v) if v==x[0] as f64));

        // x=None output:Null
        let input = [None, Some(0.0), Some(0.3)];
        let mut builder = PrimitiveVectorBuilder::with_capacity(input.len());
        for v in input {
            builder.push(v);
        }
        let x = builder.finish();
        let args: Vec<VectorRef> = vec![
            Arc::new(x),
            Arc::new(Int64Vector::from_vec(xp)),
            Arc::new(Int64Vector::from_vec(fp)),
        ];
        let vector = interp(&args).unwrap();
        assert!(matches!(vector.get(0), Value::Null));
    }
}
