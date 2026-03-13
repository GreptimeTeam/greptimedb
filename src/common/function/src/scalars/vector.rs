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
pub mod distance;
mod elem_avg;
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

use std::borrow::Cow;

use datafusion_common::{DataFusionError, Result, ScalarValue, utils};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datatypes::arrow::array::new_empty_array;

use crate::function_registry::FunctionRegistry;
use crate::scalars::vector::impl_conv::as_veclit;

pub(crate) struct VectorFunction;

impl VectorFunction {
    pub fn register(registry: &FunctionRegistry) {
        // conversion
        registry.register_scalar(convert::ParseVectorFunction::default());
        registry.register_scalar(convert::VectorToStringFunction::default());

        // distance
        registry.register_scalar(distance::CosDistanceFunction::default());
        registry.register_scalar(distance::DotProductFunction::default());
        registry.register_scalar(distance::L2SqDistanceFunction::default());

        // scalar calculation
        registry.register_scalar(scalar_add::ScalarAddFunction::default());
        registry.register_scalar(scalar_mul::ScalarMulFunction::default());

        // vector calculation
        registry.register_scalar(vector_add::VectorAddFunction::default());
        registry.register_scalar(vector_sub::VectorSubFunction::default());
        registry.register_scalar(vector_mul::VectorMulFunction::default());
        registry.register_scalar(vector_div::VectorDivFunction::default());
        registry.register_scalar(vector_norm::VectorNormFunction::default());
        registry.register_scalar(vector_dim::VectorDimFunction::default());
        registry.register_scalar(vector_kth_elem::VectorKthElemFunction::default());
        registry.register_scalar(vector_subvector::VectorSubvectorFunction::default());
        registry.register_scalar(elem_sum::ElemSumFunction::default());
        registry.register_scalar(elem_product::ElemProductFunction::default());
        registry.register_scalar(elem_avg::ElemAvgFunction::default());
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
        if len == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                args.return_field.data_type(),
            )));
        }
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
    F: Fn(&Option<Cow<[f32]>>, &Option<Cow<[f32]>>) -> Result<ScalarValue>,
{
    fn invoke_with_vectors(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg0, arg1] = utils::take_function_args(self.name, &args.args)?;

        if let (ColumnarValue::Scalar(v0), ColumnarValue::Scalar(v1)) = (arg0, arg1) {
            let v0 = as_veclit(v0)?;
            let v1 = as_veclit(v1)?;
            let result = (self.func)(&v0, &v1)?;
            return Ok(ColumnarValue::Scalar(result));
        }

        let len = ensure_same_length(&[arg0, arg1])?;
        if len == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                args.return_field.data_type(),
            )));
        }
        let mut results = Vec::with_capacity(len);

        match (arg0, arg1) {
            (ColumnarValue::Scalar(v0), ColumnarValue::Array(a1)) => {
                let v0 = as_veclit(v0)?;
                for i in 0..len {
                    let v1 = ScalarValue::try_from_array(a1, i)?;
                    let v1 = as_veclit(&v1)?;
                    results.push((self.func)(&v0, &v1)?);
                }
            }
            (ColumnarValue::Array(a0), ColumnarValue::Scalar(v1)) => {
                let v1 = as_veclit(v1)?;
                for i in 0..len {
                    let v0 = ScalarValue::try_from_array(a0, i)?;
                    let v0 = as_veclit(&v0)?;
                    results.push((self.func)(&v0, &v1)?);
                }
            }
            (ColumnarValue::Array(a0), ColumnarValue::Array(a1)) => {
                for i in 0..len {
                    let v0 = ScalarValue::try_from_array(a0, i)?;
                    let v0 = as_veclit(&v0)?;
                    let v1 = ScalarValue::try_from_array(a1, i)?;
                    let v1 = as_veclit(&v1)?;
                    results.push((self.func)(&v0, &v1)?);
                }
            }
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_)) => {
                // unreachable because this arm has been separately dealt with above
                unreachable!()
            }
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
        if len == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                args.return_field.data_type(),
            )));
        }
        let mut results = Vec::with_capacity(len);
        for i in 0..len {
            let v = ScalarValue::try_from_array(arg0, i)?;
            results.push((self.func)(&v)?);
        }

        let results = ScalarValue::iter_to_array(results.into_iter())?;
        Ok(ColumnarValue::Array(results))
    }
}

macro_rules! define_args_of_two_vector_literals_udf {
    ($(#[$attr:meta])* $name: ident) => {
        $(#[$attr])*
        #[derive(Debug, Clone)]
        pub(crate) struct $name {
            signature: datafusion_expr::Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                use arrow::datatypes::DataType;

                Self {
                    signature: crate::helper::one_of_sigs2(
                        vec![
                            DataType::Utf8,
                            DataType::Utf8View,
                            DataType::Binary,
                            DataType::BinaryView,
                        ],
                        vec![
                            DataType::Utf8,
                            DataType::Utf8View,
                            DataType::Binary,
                            DataType::BinaryView,
                        ],
                    ),
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name().to_ascii_uppercase())
            }
        }
    };
}

pub(crate) use define_args_of_two_vector_literals_udf;
