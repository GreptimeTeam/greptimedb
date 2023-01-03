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

use std::sync::Arc;

use datafusion_expr::ReturnTypeFunction as DfReturnTypeFunction;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::vectors::VectorRef;
use snafu::ResultExt;

use crate::error::{ExecuteFunctionSnafu, Result};
use crate::logical_plan::Accumulator;
use crate::prelude::{ColumnarValue, ScalarValue};

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[ConcreteDataType]) -> Result<Arc<ConcreteDataType>> + Send + Sync>;

/// Accumulator creator that will be used by DataFusion
pub type AccumulatorFunctionImpl = Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

/// Create Accumulator with the data type of input columns.
pub type AccumulatorCreatorFunction =
    Arc<dyn Fn(&[ConcreteDataType]) -> Result<Box<dyn Accumulator>> + Sync + Send>;

/// This signature corresponds to which types an aggregator serializes
/// its state, given its return datatype.
pub type StateTypeFunction =
    Arc<dyn Fn(&ConcreteDataType) -> Result<Arc<Vec<ConcreteDataType>>> + Send + Sync>;

/// decorates a function to handle [`ScalarValue`]s by converting them to arrays before calling the function
/// and vice-versa after evaluation.
pub fn make_scalar_function<F>(inner: F) -> ScalarFunctionImplementation
where
    F: Fn(&[VectorRef]) -> Result<VectorRef> + Sync + Send + 'static,
{
    Arc::new(move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an vector. If yes, store its `len`,
        // as any scalar will need to be converted to an vector of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Vector(v) => Some(v.len()),
            });

        // to array
        // TODO(dennis):  we create new vectors from Scalar on each call,
        //                            should be optimized in the future.
        let args: Result<Vec<_>> = if let Some(len) = len {
            args.iter()
                .map(|arg| arg.clone().try_into_vector(len))
                .collect()
        } else {
            args.iter()
                .map(|arg| arg.clone().try_into_vector(1))
                .collect()
        };

        let result = (inner)(&args?);

        // maybe back to scalar
        if len.is_some() {
            result.map(ColumnarValue::Vector)
        } else {
            Ok(ScalarValue::try_from_array(&result?.to_arrow_array(), 0)
                .map(ColumnarValue::Scalar)
                .context(ExecuteFunctionSnafu)?)
        }
    })
}

pub fn to_df_return_type(func: ReturnTypeFunction) -> DfReturnTypeFunction {
    let df_func = move |data_types: &[ArrowDataType]| {
        // DataFusion DataType -> ConcreteDataType
        let concrete_data_types = data_types
            .iter()
            .map(ConcreteDataType::from_arrow_type)
            .collect::<Vec<_>>();

        // evaluate ConcreteDataType
        let eval_result = (func)(&concrete_data_types);

        // ConcreteDataType -> DataFusion DataType
        eval_result
            .map(|t| Arc::new(t.as_arrow_type()))
            .map_err(|e| e.into())
    };
    Arc::new(df_func)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::vectors::BooleanVector;

    use super::*;

    #[test]
    fn test_make_scalar_function() {
        let and_fun = |args: &[VectorRef]| -> Result<VectorRef> {
            let left = &args[0]
                .as_any()
                .downcast_ref::<BooleanVector>()
                .expect("cast failed");
            let right = &args[1]
                .as_any()
                .downcast_ref::<BooleanVector>()
                .expect("cast failed");

            let result = left
                .iter_data()
                .zip(right.iter_data())
                .map(|(left, right)| match (left, right) {
                    (Some(left), Some(right)) => Some(left && right),
                    _ => None,
                })
                .collect::<BooleanVector>();
            Ok(Arc::new(result) as VectorRef)
        };

        let and_fun = make_scalar_function(and_fun);

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ColumnarValue::Vector(Arc::new(BooleanVector::from(vec![
                true, false, false, true,
            ]))),
        ];

        let vec = (and_fun)(&args).unwrap();

        match vec {
            ColumnarValue::Vector(vec) => {
                let vec = vec.as_any().downcast_ref::<BooleanVector>().unwrap();

                assert_eq!(4, vec.len());
                for i in 0..4 {
                    assert_eq!(i == 0 || i == 3, vec.get_data(i).unwrap(), "Failed at {i}")
                }
            }
            _ => unreachable!(),
        }
    }
}
