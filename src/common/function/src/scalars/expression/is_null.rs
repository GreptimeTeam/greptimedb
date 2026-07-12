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

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use datafusion::arrow::compute::is_null;
use datafusion::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

const NAME: &str = "isnull";

/// The function to check whether an expression is NULL
#[derive(Clone, Debug)]
pub(crate) struct IsNullFunction {
    signature: Signature,
}

impl Default for IsNullFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Display for IsNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for IsNullFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let result = is_null(&arg0)?;

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{AsArray, BooleanArray, Float32Array};

    use super::*;
    #[test]
    fn test_is_null_function() {
        let is_null = IsNullFunction::default();
        assert_eq!("isnull", is_null.name());
        assert_eq!(DataType::Boolean, is_null.return_type(&[]).unwrap());
        let values = vec![None, Some(3.0), None];

        let result = is_null
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(Float32Array::from(values)))],
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("", DataType::Boolean, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let vector = result.as_boolean();
        let expect = &BooleanArray::from(vec![true, false, true]);
        assert_eq!(expect, vector);
    }
}
