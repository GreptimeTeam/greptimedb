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

use arrow_cast::display::array_value_to_string;
use datafusion_common::arrow::array::{
    Array, ArrayRef, StringViewBuilder, StructArray, new_null_array,
};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use derive_more::Display;

use crate::function::Function;

#[derive(Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub struct Json2GetFunction {
    signature: Signature,
}

impl Json2GetFunction {
    pub const NAME: &'static str = "json2_get";
}

impl Function for Json2GetFunction {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!("json2_get expects 2 arguments, got {}", args.args.len());
        }

        let input = args.args[0].to_array(args.number_rows)?;
        let path = path_from_arg(&args.args[1])?;

        let segments: Vec<&str> = if path.is_empty() {
            Vec::new()
        } else {
            path.split('.').collect()
        };
        let Some(struct_path) = resolve_struct_path(&input, &segments) else {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                input.len(),
            )));
        };

        let values = display_array_from_path(&struct_path)?;
        Ok(ColumnarValue::Array(values))
    }
}

impl Default for Json2GetFunction {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        }
    }
}

fn path_from_arg(arg: &ColumnarValue) -> Result<&String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(path)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(path)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(path))) => Ok(path),
        ColumnarValue::Scalar(_) => exec_err!("json2_get expects a string path"),
        ColumnarValue::Array(_) => exec_err!("json2_get expects a literal path"),
    }
}

struct StructPath {
    parents: Vec<ArrayRef>,
    leaf: ArrayRef,
}

fn resolve_struct_path(array: &ArrayRef, segments: &[&str]) -> Option<StructPath> {
    let mut current = array.clone();
    let mut parents = Vec::with_capacity(segments.len());

    for segment in segments {
        let struct_array = current.as_any().downcast_ref::<StructArray>()?;
        let DataType::Struct(fields) = current.data_type() else {
            unreachable!()
        };
        let index = fields.iter().position(|field| field.name() == *segment)?;
        parents.push(current.clone());
        current = struct_array.column(index).clone();
    }

    Some(StructPath {
        parents,
        leaf: current,
    })
}

fn struct_path_is_null(parents: &[ArrayRef], index: usize) -> bool {
    parents.iter().any(|parent| parent.is_null(index))
}

fn display_array_from_path(path: &StructPath) -> Result<ArrayRef> {
    let mut builder = StringViewBuilder::with_capacity(path.leaf.len());
    for index in 0..path.leaf.len() {
        if struct_path_is_null(&path.parents, index) || path.leaf.is_null(index) {
            builder.append_null();
            continue;
        }

        let value = array_value_to_string(path.leaf.as_ref(), index)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        builder.append_value(value);
    }
    Ok(Arc::new(builder.finish()))
}
