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

use common_query::error::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use session::context::Channel;

use crate::function::{Function, find_function_context};

#[derive(Clone, Debug, Default)]
pub(crate) struct VersionFunction;

impl fmt::Display for VersionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VERSION")
    }
}

impl Function for VersionFunction {
    fn name(&self) -> &str {
        "version"
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
        let version = match func_ctx.query_ctx.channel() {
            Channel::Mysql => {
                format!(
                    "{}-greptimedb-{}",
                    std::env::var("GREPTIMEDB_MYSQL_SERVER_VERSION")
                        .unwrap_or_else(|_| "8.4.2".to_string()),
                    common_version::version()
                )
            }
            Channel::Postgres => {
                format!("16.3-greptimedb-{}", common_version::version())
            }
            _ => common_version::version().to_string(),
        };
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(version))))
    }
}
