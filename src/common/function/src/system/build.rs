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

use datafusion::arrow::array::StringViewArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;
use crate::system::define_nullary_udf;

define_nullary_udf!(
/// Generates build information
BuildFunction);

impl Function for BuildFunction {
    fn name(&self) -> &str {
        "build"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion_common::Result<ColumnarValue> {
        let build_info = common_version::build_info().to_string();
        Ok(ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            build_info,
        ]))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::ArrayRef;
    use datafusion_common::config::ConfigOptions;

    use super::*;
    #[test]
    fn test_build_function() {
        let build = BuildFunction::default();
        assert_eq!("build", build.name());
        assert_eq!(DataType::Utf8View, build.return_type(&[]).unwrap());
        let build_info = common_version::build_info().to_string();
        let actual = build
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
                config_options: Arc::new(ConfigOptions::new()),
            })
            .unwrap();
        let actual = ColumnarValue::values_to_arrays(&[actual]).unwrap();
        let expect = vec![Arc::new(StringViewArray::from(vec![build_info])) as ArrayRef];
        assert_eq!(actual, expect);
    }
}
