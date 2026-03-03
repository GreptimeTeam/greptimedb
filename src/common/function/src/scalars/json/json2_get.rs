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

use arrow::array::StringViewBuilder;
use arrow_cast::display::ArrayFormatter;
use datafusion_common::arrow::array::{Array, ArrayRef, StructArray, new_null_array};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, Signature, Volatility,
};
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
        internal_err!("this method isn't meant to be called")
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("json2_get expects 3 arguments, got {}", args.args.len());
        }

        let input = args.args[0].to_array(args.number_rows)?;
        let path = path_from_arg(&args.args[1])?;
        let return_type = args.return_field.data_type();

        let segments = path.split('.').collect::<Vec<_>>();
        let Some(leaf) = resolve_leaf_path(&input, &segments) else {
            return Ok(ColumnarValue::Array(new_null_array(
                return_type,
                input.len(),
            )));
        };

        let casted = if arrow_cast::can_cast_types(leaf.data_type(), return_type) {
            arrow_cast::cast(leaf.as_ref(), return_type)?
        } else if return_type.is_string() {
            cast_array_to_string(&leaf)?
        } else {
            return Ok(ColumnarValue::Array(new_null_array(
                return_type,
                input.len(),
            )));
        };

        Ok(ColumnarValue::Array(casted))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let Some(Some(value)) = args.scalar_arguments.get(2) else {
            return internal_err!(
                "third argument of function {} must be present and is scalar",
                self.name()
            );
        };
        Ok(Arc::new(Field::new(
            "json2_get expected type",
            value.data_type(),
            true,
        )))
    }
}

impl Default for Json2GetFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
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

fn resolve_leaf_path(array: &ArrayRef, segments: &[&str]) -> Option<ArrayRef> {
    if segments.is_empty() {
        return None;
    }

    let mut current = array.clone();
    for segment in segments {
        let struct_array = current.as_any().downcast_ref::<StructArray>()?;
        let DataType::Struct(fields) = current.data_type() else {
            unreachable!()
        };
        let index = fields.iter().position(|field| field.name() == *segment)?;
        current = struct_array.column(index).clone();
    }
    Some(current)
}

fn cast_array_to_string(array: &ArrayRef) -> Result<ArrayRef> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let formatter = ArrayFormatter::try_new(array, &Default::default())?;
    for i in 0..array.len() {
        let value = array.is_valid(i).then(|| formatter.value(i).to_string());
        builder.append_option(value);
    }
    Ok(Arc::new(builder.finish()))
}

pub fn datatype_expr(data_type: &DataType) -> Result<Expr> {
    ScalarValue::try_new_null(data_type).map(|x| Expr::Literal(x, None))
}
