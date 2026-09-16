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

use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, find_function_context};
use crate::system::define_nullary_udf;

define_nullary_udf!(DatabaseFunction);
define_nullary_udf!(SchemaFunction);
define_nullary_udf!(UserFunction);
define_nullary_udf!(CurrentUserFunction);
define_nullary_udf!(SystemUserFunction);
define_nullary_udf!(ReadPreferenceFunction);
define_nullary_udf!(PgBackendPidFunction);
define_nullary_udf!(ConnectionIdFunction);

const DATABASE_FUNCTION_NAME: &str = "database";
const SCHEMA_FUNCTION_NAME: &str = "schema";
const USER_FUNCTION_NAME: &str = "user";
const CURRENT_USER_FUNCTION_NAME: &str = "current_user";
const SYSTEM_USER_FUNCTION_NAME: &str = "system_user";
const READ_PREFERENCE_FUNCTION_NAME: &str = "read_preference";
const PG_BACKEND_PID: &str = "pg_backend_pid";
const CONNECTION_ID: &str = "connection_id";

macro_rules! impl_current_schema_function {
    ($name: ident, $fn_name: expr) => {
        impl Function for $name {
            fn name(&self) -> &str {
                $fn_name
            }

            fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
                Ok(DataType::Utf8View)
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion_common::Result<ColumnarValue> {
                let func_ctx = find_function_context(&args)?;
                let db = func_ctx.query_ctx.current_schema();

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(db))))
            }
        }
    };
}

impl_current_schema_function!(DatabaseFunction, DATABASE_FUNCTION_NAME);
// MySQL's `SCHEMA()` is a synonym for `DATABASE()`.
impl_current_schema_function!(SchemaFunction, SCHEMA_FUNCTION_NAME);

macro_rules! impl_current_user_function {
    ($name: ident, $fn_name: expr) => {
        impl Function for $name {
            fn name(&self) -> &str {
                $fn_name
            }

            fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
                Ok(DataType::Utf8View)
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion_common::Result<ColumnarValue> {
                let func_ctx = find_function_context(&args)?;
                let user = func_ctx.query_ctx.current_user();

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
                    user.username().to_string(),
                ))))
            }
        }
    };
}

// GreptimeDB has no notion of a user switching identity mid-session, so `USER()`,
// `CURRENT_USER()` and `SYSTEM_USER()` all report the authenticated user.
impl_current_user_function!(UserFunction, USER_FUNCTION_NAME);
impl_current_user_function!(CurrentUserFunction, CURRENT_USER_FUNCTION_NAME);
impl_current_user_function!(SystemUserFunction, SYSTEM_USER_FUNCTION_NAME);

impl Function for ReadPreferenceFunction {
    fn name(&self) -> &str {
        READ_PREFERENCE_FUNCTION_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let read_preference = func_ctx.query_ctx.read_preference();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
            read_preference.to_string(),
        ))))
    }
}

impl Function for PgBackendPidFunction {
    fn name(&self) -> &str {
        PG_BACKEND_PID
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let pid = func_ctx.query_ctx.process_id();

        Ok(ColumnarValue::Scalar(ScalarValue::UInt64(Some(pid as u64))))
    }
}

impl Function for ConnectionIdFunction {
    fn name(&self) -> &str {
        CONNECTION_ID
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let func_ctx = find_function_context(&args)?;
        let pid = func_ctx.query_ctx.process_id();

        Ok(ColumnarValue::Scalar(ScalarValue::UInt32(Some(pid))))
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
        let build = DatabaseFunction::default();
        assert_eq!("database", build.name());
        assert_eq!(DataType::Utf8View, build.return_type(&[]).unwrap());

        let query_ctx = QueryContextBuilder::default()
            .current_schema("test_db".to_string())
            .build()
            .into();

        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext {
            query_ctx,
            ..Default::default()
        });
        let config_options = Arc::new(config_options);

        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
            config_options,
        };
        let result = build.invoke_with_args(args).unwrap();
        let ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) = result else {
            unreachable!()
        };
        assert_eq!(s, "test_db");
    }
}
