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

use std::fmt::{self};

use common_query::error::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, find_function_context};

/// A function to return current session timezone.
#[derive(Clone, Debug, Default)]
pub struct TimezoneFunction;

const NAME: &str = "timezone";

impl Function for TimezoneFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let tz = func_ctx.query_ctx.timezone().to_string();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(tz))))
    }
}

impl fmt::Display for TimezoneFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TIMEZONE")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use session::context::QueryContextBuilder;

    use super::*;
    use crate::function::FunctionContext;

    #[test]
    fn test_build_function() {
        let build = TimezoneFunction;
        assert_eq!("timezone", build.name());
        assert_eq!(DataType::Utf8View, build.return_type(&[]).unwrap());
        assert_eq!(build.signature(), Signature::nullary(Volatility::Immutable));

        let query_ctx = QueryContextBuilder::default().build().into();

        let func_ctx = FunctionContext {
            query_ctx,
            ..Default::default()
        };
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(func_ctx);
        let config_options = Arc::new(config_options);

        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options,
        };
        let result = build.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) = result else {
            unreachable!()
        };
        assert_eq!(s, "UTC");
    }
}
