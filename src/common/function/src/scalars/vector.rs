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

mod convert;
mod distance;
mod elem_product;
mod elem_sum;
pub mod impl_conv;
mod scalar_add;
mod scalar_mul;
mod vector_add;
mod vector_dim;
mod vector_div;
mod vector_kth_elem;
mod vector_mul;
mod vector_norm;
mod vector_sub;
mod vector_subvector;

use datafusion_common::{DataFusionError, Result, ScalarValue, utils};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

use crate::function_registry::FunctionRegistry;

pub(crate) struct VectorFunction;

impl VectorFunction {
    pub fn register(registry: &FunctionRegistry) {
        // conversion
        registry.register_scalar(convert::ParseVectorFunction);
        registry.register_scalar(convert::VectorToStringFunction);

        // distance
        registry.register_scalar(distance::CosDistanceFunction);
        registry.register_scalar(distance::DotProductFunction);
        registry.register_scalar(distance::L2SqDistanceFunction);

        // scalar calculation
        registry.register_scalar(scalar_add::ScalarAddFunction);
        registry.register_scalar(scalar_mul::ScalarMulFunction);

        // vector calculation
        registry.register_scalar(vector_add::VectorAddFunction);
        registry.register_scalar(vector_sub::VectorSubFunction);
        registry.register_scalar(vector_mul::VectorMulFunction);
        registry.register_scalar(vector_div::VectorDivFunction);
        registry.register_scalar(vector_norm::VectorNormFunction);
        registry.register_scalar(vector_dim::VectorDimFunction);
        registry.register_scalar(vector_kth_elem::VectorKthElemFunction);
        registry.register_scalar(vector_subvector::VectorSubvectorFunction);
        registry.register_scalar(elem_sum::ElemSumFunction);
        registry.register_scalar(elem_product::ElemProductFunction);
    }
}

// Use macro instead of function to "return" the reference to `ScalarValue` in the
// `ColumnarValue::Array` match arm.
macro_rules! try_get_scalar_value {
    ($col: ident, $i: ident) => {
        match $col {
            datafusion::logical_expr::ColumnarValue::Array(a) => {
                &datafusion_common::ScalarValue::try_from_array(a.as_ref(), $i)?
            }
            datafusion::logical_expr::ColumnarValue::Scalar(v) => v,
        }
    };
}

pub(crate) fn ensure_same_length(values: &[&ColumnarValue]) -> Result<usize> {
    if values.is_empty() {
        return Ok(0);
    }

    let mut array_len = None;
    for v in values {
        array_len = match (v, array_len) {
            (ColumnarValue::Array(a), None) => Some(a.len()),
            (ColumnarValue::Array(a), Some(array_len)) => {
                if array_len == a.len() {
                    Some(array_len)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Arguments has mixed length. Expected length: {array_len}, found length: {}",
                        a.len()
                    )));
                }
            }
            (ColumnarValue::Scalar(_), array_len) => array_len,
        }
    }

    // If array_len is none, it means there are only scalars, treat them each as 1 element array.
    let array_len = array_len.unwrap_or(1);
    Ok(array_len)
}

struct VectorCalculator<'a, F> {
    name: &'a str,
    func: F,
}

impl<F> VectorCalculator<'_, F>
where
    F: Fn(&ScalarValue, &ScalarValue) -> Result<ScalarValue>,
{
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg0, arg1] = utils::take_function_args(self.name, &args.args)?;

        if let (ColumnarValue::Scalar(v0), ColumnarValue::Scalar(v1)) = (arg0, arg1) {
            let result = (self.func)(v0, v1)?;
            return Ok(ColumnarValue::Scalar(result));
        }

        let len = ensure_same_length(&[arg0, arg1])?;
        let mut results = Vec::with_capacity(len);
        for i in 0..len {
            let v0 = try_get_scalar_value!(arg0, i);
            let v1 = try_get_scalar_value!(arg1, i);
            results.push((self.func)(v0, v1)?);
        }

        let results = ScalarValue::iter_to_array(results.into_iter())?;
        Ok(ColumnarValue::Array(results))
    }
}

impl<F> VectorCalculator<'_, F>
where
    F: Fn(&ScalarValue) -> Result<ScalarValue>,
{
    fn invoke_with_single_argument(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg0] = utils::take_function_args(self.name, &args.args)?;

        let arg0 = match arg0 {
            ColumnarValue::Scalar(v) => {
                let result = (self.func)(v)?;
                return Ok(ColumnarValue::Scalar(result));
            }
            ColumnarValue::Array(a) => a,
        };

        let len = arg0.len();
        let mut results = Vec::with_capacity(len);
        for i in 0..len {
            let v = ScalarValue::try_from_array(arg0, i)?;
            results.push((self.func)(&v)?);
        }

        let results = ScalarValue::iter_to_array(results.into_iter())?;
        Ok(ColumnarValue::Array(results))
    }
}
