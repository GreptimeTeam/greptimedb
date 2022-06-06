use std::sync::Arc;

use common_query::error::{ExecuteFunctionSnafu, FromScalarValueSnafu};
use common_query::prelude::ScalarValue;
use common_query::prelude::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUdf,
};
use datatypes::error::Error as DataTypeError;
use datatypes::prelude::{ConcreteDataType, VectorHelper};
use snafu::ResultExt;

use crate::scalars::function::{Function, FunctionContext};

/// Create a ScalarUdf from function.
pub fn create_udf(func: Arc<dyn Function>) -> ScalarUdf {
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
                ColumnarValue::Scalar(v) => VectorHelper::try_from_scalar_value(v.clone(), rows),
                ColumnarValue::Vector(v) => Ok(v.clone()),
            })
            .collect();

        let result = func_cloned.eval(func_ctx, &args.context(FromScalarValueSnafu)?);

        if len.is_some() {
            result.map(ColumnarValue::Vector).map_err(|e| e.into())
        } else {
            ScalarValue::try_from_array(&result?.to_arrow_array(), 0)
                .map(ColumnarValue::Scalar)
                .context(ExecuteFunctionSnafu)
        }
    });

    ScalarUdf::new(func.name(), &func.signature(), &return_type, &fun)
}
