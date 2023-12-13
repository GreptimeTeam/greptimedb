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

use common_query::error::FromScalarValueSnafu;
use common_query::prelude::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUdf,
};
use datatypes::error::Error as DataTypeError;
use datatypes::prelude::*;
use datatypes::vectors::Helper;
use snafu::ResultExt;

use crate::function::{FunctionContext, FunctionRef};

/// Create a ScalarUdf from function.
pub fn create_udf(func: FunctionRef) -> ScalarUdf {
    let func_cloned = func.clone();
    let return_type: ReturnTypeFunction = Arc::new(move |input_types: &[ConcreteDataType]| {
        Ok(Arc::new(func_cloned.return_type(input_types)?))
    });

    let func_cloned = func.clone();
    let fun: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        let func_ctx = FunctionContext::default();

        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Vector(v) => Some(v.len()),
            });

        let rows = len.unwrap_or(1);

        let args: Result<Vec<_>, DataTypeError> = args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Scalar(v) => Helper::try_from_scalar_value(v.clone(), rows),
                ColumnarValue::Vector(v) => Ok(v.clone()),
            })
            .collect();

        let result = func_cloned.eval(func_ctx, &args.context(FromScalarValueSnafu)?);
        let udf_result = result.map(ColumnarValue::Vector)?;
        Ok(udf_result)
    });

    ScalarUdf::new(func.name(), &func.signature(), &return_type, &fun)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::{ColumnarValue, ScalarValue};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::{ScalarVector, Vector, VectorRef};
    use datatypes::value::Value;
    use datatypes::vectors::{BooleanVector, ConstantVector};

    use super::*;
    use crate::function::Function;
    use crate::scalars::test::TestAndFunction;

    #[test]
    fn test_create_udf() {
        let f = Arc::new(TestAndFunction);

        let args: Vec<VectorRef> = vec![
            Arc::new(ConstantVector::new(
                Arc::new(BooleanVector::from(vec![true])),
                3,
            )),
            Arc::new(BooleanVector::from(vec![true, false, true])),
        ];

        let vector = f.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(3, vector.len());

        for i in 0..3 {
            assert!(matches!(vector.get(i), Value::Boolean(b) if b == (i == 0 || i == 2)));
        }

        // create a udf and test it again
        let udf = create_udf(f.clone());

        assert_eq!("test_and", udf.name);
        assert_eq!(f.signature(), udf.signature);
        assert_eq!(
            Arc::new(ConcreteDataType::boolean_datatype()),
            ((udf.return_type)(&[])).unwrap()
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ColumnarValue::Vector(Arc::new(BooleanVector::from(vec![
                true, false, false, true,
            ]))),
        ];

        let vec = (udf.fun)(&args).unwrap();

        match vec {
            ColumnarValue::Vector(vec) => {
                let vec = vec.as_any().downcast_ref::<BooleanVector>().unwrap();

                assert_eq!(4, vec.len());
                for i in 0..4 {
                    assert_eq!(i == 0 || i == 3, vec.get_data(i).unwrap(), "Failed at {i}",)
                }
            }
            _ => unreachable!(),
        }
    }
}
