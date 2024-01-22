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
use datatypes::prelude::{ScalarVector, VectorRef};
use datatypes::types::LogicalPrimitiveType;
use datatypes::vectors::PrimitiveVector;
use datatypes::with_match_primitive_type_id;
use snafu::{ensure, OptionExt};

use crate::function::Function;

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

    fn eval(
        &self,
        _func_ctx: crate::function::FunctionContext,
        columns: &[VectorRef],
    ) -> Result<VectorRef> {
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
            let input = columns[0]
                .as_any()
                .downcast_ref::<PrimitiveVector<$S>>()
                .unwrap()
                .as_arrow();
            let min = columns[1]
                .as_any()
                .downcast_ref::<PrimitiveVector<$S>>()
                .unwrap()
                .get_data(0)
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The second arg should not be none",
                    }
                })?;
            let max = columns[2]
                .as_any()
                .downcast_ref::<PrimitiveVector<$S>>()
                .unwrap()
                .get_data(0)
                .with_context(|| {
                    InvalidFuncArgsSnafu {
                        err_msg: "The third arg should not be none",
                    }
                })?;
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
