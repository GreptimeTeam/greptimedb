// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{self, Display};
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::Signature;
use datafusion::arrow::array::{ArrayIter, PrimitiveArray};
use datafusion::logical_expr::Volatility;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::VectorRef;
use datatypes::types::LogicalPrimitiveType;
use datatypes::value::TryAsPrimitive;
use datatypes::vectors::PrimitiveVector;
use datatypes::with_match_primitive_type_id;
use snafu::{ensure, OptionExt};

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct ClampFunction;

const CLAMP_NAME: &str = "clamp";

impl Function for ClampFunction {
    fn name(&self) -> &str {
        CLAMP_NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        // Type check is done by `signature`
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, min, max
        Signature::uniform(3, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 3,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 3, have: {}",
                    columns.len()
                ),
            }
        );
        ensure!(
            columns[0].data_type().is_numeric(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The first arg's type is not numeric, have: {}",
                    columns[0].data_type()
                ),
            }
        );
        ensure!(
            columns[0].data_type() == columns[1].data_type()
                && columns[1].data_type() == columns[2].data_type(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "Arguments don't have identical types: {}, {}, {}",
                    columns[0].data_type(),
                    columns[1].data_type(),
                    columns[2].data_type()
                ),
            }
        );
        ensure!(
            columns[1].len() == 1 && columns[2].len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The second and third args should be scalar, have: {:?}, {:?}",
                    columns[1], columns[2]
                ),
            }
        );

        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            let input_array = columns[0].to_arrow_array();
            let input = input_array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<<$S as LogicalPrimitiveType>::ArrowPrimitive>>()
                    .unwrap();

            let min = TryAsPrimitive::<$S>::try_as_primitive(&columns[1].get(0))
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The second arg should not be none",
                    }
                })?;
            let max = TryAsPrimitive::<$S>::try_as_primitive(&columns[2].get(0))
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The third arg should not be none",
                    }
                })?;

            // ensure min <= max
            ensure!(
                min <= max,
                    InvalidFuncArgsSnafu {
                        err_msg: format!(
                        "The second arg should be less than or equal to the third arg, have: {:?}, {:?}",
                        columns[1], columns[2]
                    ),
                }
            );

            clamp_impl::<$S, true, true>(input, min, max)
        },{
            unreachable!()
        })
    }
}

impl Display for ClampFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_NAME.to_ascii_uppercase())
    }
}

fn clamp_impl<T: LogicalPrimitiveType, const CLAMP_MIN: bool, const CLAMP_MAX: bool>(
    input: &PrimitiveArray<T::ArrowPrimitive>,
    min: T::Native,
    max: T::Native,
) -> Result<VectorRef> {
    let iter = ArrayIter::new(input);
    let result = iter.map(|x| {
        x.map(|x| {
            if CLAMP_MIN && x < min {
                min
            } else if CLAMP_MAX && x > max {
                max
            } else {
                x
            }
        })
    });
    let result = PrimitiveArray::<T::ArrowPrimitive>::from_iter(result);
    Ok(Arc::new(PrimitiveVector::<T>::from(result)))
}

#[derive(Clone, Debug, Default)]
pub struct ClampMinFunction;

const CLAMP_MIN_NAME: &str = "clamp_min";

impl Function for ClampMinFunction {
    fn name(&self) -> &str {
        CLAMP_MIN_NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, min
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );
        ensure!(
            columns[0].data_type().is_numeric(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The first arg's type is not numeric, have: {}",
                    columns[0].data_type()
                ),
            }
        );
        ensure!(
            columns[0].data_type() == columns[1].data_type(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "Arguments don't have identical types: {}, {}",
                    columns[0].data_type(),
                    columns[1].data_type()
                ),
            }
        );
        ensure!(
            columns[1].len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The second arg (min) should be scalar, have: {:?}",
                    columns[1]
                ),
            }
        );

        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            let input_array = columns[0].to_arrow_array();
            let input = input_array
                .as_any()
                .downcast_ref::<PrimitiveArray<<$S as LogicalPrimitiveType>::ArrowPrimitive>>()
                .unwrap();

            let min = TryAsPrimitive::<$S>::try_as_primitive(&columns[1].get(0))
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The second arg (min) should not be none",
                    }
                })?;
            // For clamp_min, max is effectively infinity, so we don't use it in the clamp_impl logic.
            // We pass a default/dummy value for max.
            let max_dummy = <$S as LogicalPrimitiveType>::Native::default();

            clamp_impl::<$S, true, false>(input, min, max_dummy)
        },{
            unreachable!()
        })
    }
}

impl Display for ClampMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_MIN_NAME.to_ascii_uppercase())
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClampMaxFunction;

const CLAMP_MAX_NAME: &str = "clamp_max";

impl Function for ClampMaxFunction {
    fn name(&self) -> &str {
        CLAMP_MAX_NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, max
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly 2, have: {}",
                    columns.len()
                ),
            }
        );
        ensure!(
            columns[0].data_type().is_numeric(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The first arg's type is not numeric, have: {}",
                    columns[0].data_type()
                ),
            }
        );
        ensure!(
            columns[0].data_type() == columns[1].data_type(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "Arguments don't have identical types: {}, {}",
                    columns[0].data_type(),
                    columns[1].data_type()
                ),
            }
        );
        ensure!(
            columns[1].len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The second arg (max) should be scalar, have: {:?}",
                    columns[1]
                ),
            }
        );

        with_match_primitive_type_id!(columns[0].data_type().logical_type_id(), |$S| {
            let input_array = columns[0].to_arrow_array();
            let input = input_array
                .as_any()
                .downcast_ref::<PrimitiveArray<<$S as LogicalPrimitiveType>::ArrowPrimitive>>()
                .unwrap();

            let max = TryAsPrimitive::<$S>::try_as_primitive(&columns[1].get(0))
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The second arg (max) should not be none",
                    }
                })?;
            // For clamp_max, min is effectively -infinity, so we don't use it in the clamp_impl logic.
            // We pass a default/dummy value for min.
            let min_dummy = <$S as LogicalPrimitiveType>::Native::default();

            clamp_impl::<$S, false, true>(input, min_dummy, max)
        },{
            unreachable!()
        })
    }
}

impl Display for ClampMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_MAX_NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use datatypes::prelude::ScalarVector;
    use datatypes::vectors::{
        ConstantVector, Float64Vector, Int64Vector, StringVector, UInt64Vector,
    };

    use super::*;
    use crate::function::FunctionContext;

    #[test]
    fn clamp_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                -1,
                10,
                vec![Some(-1), Some(-1), Some(-1), Some(0), Some(1), Some(2)],
            ),
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                0,
                0,
                vec![Some(0), Some(0), Some(0), Some(0), Some(0), Some(0)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                -2,
                1,
                vec![Some(-2), None, Some(-1), None, None, Some(1)],
            ),
            (
                vec![None, None, None, None, None],
                0,
                1,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let args = [
                Arc::new(Int64Vector::from(in_data)) as _,
                Arc::new(Int64Vector::from_vec(vec![min])) as _,
                Arc::new(Int64Vector::from_vec(vec![max])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Int64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_u64() {
        let inputs = [
            (
                vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
                1,
                3,
                vec![Some(1), Some(1), Some(2), Some(3), Some(3), Some(3)],
            ),
            (
                vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
                0,
                0,
                vec![Some(0), Some(0), Some(0), Some(0), Some(0), Some(0)],
            ),
            (
                vec![Some(0), None, Some(2), None, None, Some(5)],
                1,
                3,
                vec![Some(1), None, Some(2), None, None, Some(3)],
            ),
            (
                vec![None, None, None, None, None],
                0,
                1,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let args = [
                Arc::new(UInt64Vector::from(in_data)) as _,
                Arc::new(UInt64Vector::from_vec(vec![min])) as _,
                Arc::new(UInt64Vector::from_vec(vec![max])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(UInt64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_f64() {
        let inputs = [
            (
                vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
                -1.0,
                10.0,
                vec![Some(-1.0), Some(-1.0), Some(-1.0), Some(0.0), Some(1.0)],
            ),
            (
                vec![Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
                0.0,
                0.0,
                vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
            ),
            (
                vec![Some(-3.0), None, Some(-1.0), None, None, Some(2.0)],
                -2.0,
                1.0,
                vec![Some(-2.0), None, Some(-1.0), None, None, Some(1.0)],
            ),
            (
                vec![None, None, None, None, None],
                0.0,
                1.0,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let args = [
                Arc::new(Float64Vector::from(in_data)) as _,
                Arc::new(Float64Vector::from_vec(vec![min])) as _,
                Arc::new(Float64Vector::from_vec(vec![max])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Float64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_const_i32() {
        let input = vec![Some(5)];
        let min = 2;
        let max = 4;

        let func = ClampFunction;
        let args = [
            Arc::new(ConstantVector::new(Arc::new(Int64Vector::from(input)), 1)) as _,
            Arc::new(Int64Vector::from_vec(vec![min])) as _,
            Arc::new(Int64Vector::from_vec(vec![max])) as _,
        ];
        let result = func
            .eval(&FunctionContext::default(), args.as_slice())
            .unwrap();
        let expected: VectorRef = Arc::new(Int64Vector::from(vec![Some(4)]));
        assert_eq!(expected, result);
    }

    #[test]
    fn clamp_invalid_min_max() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = 10.0;
        let max = -1.0;

        let func = ClampFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Float64Vector::from_vec(vec![min])) as _,
            Arc::new(Float64Vector::from_vec(vec![max])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -1;
        let max = 10;

        let func = ClampFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Int64Vector::from_vec(vec![min])) as _,
            Arc::new(UInt64Vector::from_vec(vec![max])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_min_is_not_scalar() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -10.0;
        let max = 1.0;

        let func = ClampFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Float64Vector::from_vec(vec![min, min])) as _,
            Arc::new(Float64Vector::from_vec(vec![max])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_no_max() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -10.0;

        let func = ClampFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Float64Vector::from_vec(vec![min])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_on_string() {
        let input = vec![Some("foo"), Some("foo"), Some("foo"), Some("foo")];

        let func = ClampFunction;
        let args = [
            Arc::new(StringVector::from(input)) as _,
            Arc::new(StringVector::from_vec(vec!["bar"])) as _,
            Arc::new(StringVector::from_vec(vec!["baz"])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_min_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                -1,
                vec![Some(-1), Some(-1), Some(-1), Some(0), Some(1), Some(2)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                -2,
                vec![Some(-2), None, Some(-1), None, None, Some(2)],
            ),
        ];

        let func = ClampMinFunction;
        for (in_data, min, expected) in inputs {
            let args = [
                Arc::new(Int64Vector::from(in_data)) as _,
                Arc::new(Int64Vector::from_vec(vec![min])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Int64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_max_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                1,
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(1)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                0,
                vec![Some(-3), None, Some(-1), None, None, Some(0)],
            ),
        ];

        let func = ClampMaxFunction;
        for (in_data, max, expected) in inputs {
            let args = [
                Arc::new(Int64Vector::from(in_data)) as _,
                Arc::new(Int64Vector::from_vec(vec![max])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Int64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_min_f64() {
        let inputs = [(
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
            -1.0,
            vec![Some(-1.0), Some(-1.0), Some(-1.0), Some(0.0), Some(1.0)],
        )];

        let func = ClampMinFunction;
        for (in_data, min, expected) in inputs {
            let args = [
                Arc::new(Float64Vector::from(in_data)) as _,
                Arc::new(Float64Vector::from_vec(vec![min])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Float64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_max_f64() {
        let inputs = [(
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
            0.0,
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(0.0)],
        )];

        let func = ClampMaxFunction;
        for (in_data, max, expected) in inputs {
            let args = [
                Arc::new(Float64Vector::from(in_data)) as _,
                Arc::new(Float64Vector::from_vec(vec![max])) as _,
            ];
            let result = func
                .eval(&FunctionContext::default(), args.as_slice())
                .unwrap();
            let expected: VectorRef = Arc::new(Float64Vector::from(expected));
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn clamp_min_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -1;

        let func = ClampMinFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Int64Vector::from_vec(vec![min])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn clamp_max_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let max = 1;

        let func = ClampMaxFunction;
        let args = [
            Arc::new(Float64Vector::from(input)) as _,
            Arc::new(Int64Vector::from_vec(vec![max])) as _,
        ];
        let result = func.eval(&FunctionContext::default(), args.as_slice());
        assert!(result.is_err());
    }
}
