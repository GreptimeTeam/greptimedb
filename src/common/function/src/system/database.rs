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
use derive_more::Display;

use crate::function::{Function, find_function_context};

/// A function to return current schema name.
#[derive(Clone, Debug, Default)]
pub struct DatabaseFunction;

#[derive(Clone, Debug, Default)]
pub struct CurrentSchemaFunction;
pub struct SessionUserFunction;

pub struct ReadPreferenceFunction;

#[derive(Display)]
#[display("{}", self.name())]
pub struct PgBackendPidFunction;

#[derive(Display)]
#[display("{}", self.name())]
pub struct ConnectionIdFunction;

const DATABASE_FUNCTION_NAME: &str = "database";
const CURRENT_SCHEMA_FUNCTION_NAME: &str = "current_schema";
const SESSION_USER_FUNCTION_NAME: &str = "session_user";
const READ_PREFERENCE_FUNCTION_NAME: &str = "read_preference";
const PG_BACKEND_PID: &str = "pg_backend_pid";
const CONNECTION_ID: &str = "connection_id";

impl Function for DatabaseFunction {
    fn name(&self) -> &str {
        DATABASE_FUNCTION_NAME
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
        let db = func_ctx.query_ctx.current_schema();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(db))))
    }
}

// Though "current_schema" can be aliased to "database", to not cause any breaking changes,
// we are not doing it: not until https://github.com/apache/datafusion/issues/17469 is resolved.
impl Function for CurrentSchemaFunction {
    fn name(&self) -> &str {
        CURRENT_SCHEMA_FUNCTION_NAME
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
        let db = func_ctx.query_ctx.current_schema();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(db))))
    }
}

impl Function for SessionUserFunction {
    fn name(&self) -> &str {
        SESSION_USER_FUNCTION_NAME
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
        let user = func_ctx.query_ctx.current_user();

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(
            user.username().to_string(),
        ))))
    }
}

impl Function for ReadPreferenceFunction {
    fn name(&self) -> &str {
        READ_PREFERENCE_FUNCTION_NAME
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

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
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

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn signature(&self) -> Signature {
        Signature::nullary(Volatility::Immutable)
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

impl fmt::Display for DatabaseFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DATABASE")
    }
}

impl fmt::Display for CurrentSchemaFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CURRENT_SCHEMA")
    }
}

impl fmt::Display for SessionUserFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SESSION_USER")
    }
}

impl fmt::Display for ReadPreferenceFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "READ_PREFERENCE")
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
        let build = DatabaseFunction;
        assert_eq!("database", build.name());
        assert_eq!(DataType::Utf8View, build.return_type(&[]).unwrap());
        assert_eq!(build.signature(), Signature::nullary(Volatility::Immutable));

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
