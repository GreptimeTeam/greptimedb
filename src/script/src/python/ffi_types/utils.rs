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

// to avoid put too many #cfg for pyo3 feature flag
#![allow(unused)]
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::Field;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datatypes::arrow::datatypes::DataType as ArrowDataType;

pub fn new_item_field(data_type: ArrowDataType) -> Field {
    Field::new("item", data_type, false)
}

/// Generate friendly error message when the type of the input `values` is different than `ty`
/// # Example
/// `values` is [Int64(1), Float64(1.0), Int64(2)] and `ty` is Int64
/// then the error message will be: " Float64 at 2th location\n"
pub(crate) fn collect_diff_types_string(values: &[ScalarValue], ty: &ArrowDataType) -> String {
    values
        .iter()
        .enumerate()
        .filter_map(|(idx, val)| {
            if val.data_type() != *ty {
                Some((idx, val.data_type()))
            } else {
                None
            }
        })
        .map(|(idx, ty)| format!(" {:?} at {}th location\n", ty, idx + 1))
        .reduce(|mut acc, item| {
            acc.push_str(&item);
            acc
        })
        .unwrap_or_else(|| "Nothing".to_string())
}

/// Because most of the datafusion's UDF only support f32/64, so cast all to f64 to use datafusion's UDF
pub fn all_to_f64(col: ColumnarValue) -> Result<ColumnarValue, String> {
    match col {
        ColumnarValue::Array(arr) => {
            let res = compute::cast(&arr, &ArrowDataType::Float64).map_err(|err| {
                format!(
                    "Arrow Type Cast Fail(from {:#?} to {:#?}): {err:#?}",
                    arr.data_type(),
                    ArrowDataType::Float64
                )
            })?;
            Ok(ColumnarValue::Array(res))
        }
        ColumnarValue::Scalar(val) => {
            let val_in_f64 = match val {
                ScalarValue::Float64(Some(v)) => v,
                ScalarValue::Int64(Some(v)) => v as f64,
                ScalarValue::Boolean(Some(v)) => v as i64 as f64,
                _ => {
                    return Err(format!(
                        "Can't cast type {:#?} to {:#?}",
                        val.data_type(),
                        ArrowDataType::Float64
                    ))
                }
            };
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                val_in_f64,
            ))))
        }
    }
}
