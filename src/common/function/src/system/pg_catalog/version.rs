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

use crate::function::Function;

#[derive(Clone, Debug, Default)]
pub(crate) struct PGVersionFunction;

impl fmt::Display for PGVersionFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "pg_catalog.VERSION")
    }
}

impl Function for PGVersionFunction {
    fn name(&self) -> &str {
        "pg_catalog.version"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        Signature::exact(vec![], Volatility::Immutable)
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion_common::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(Some(format!(
            "PostgreSQL 16.3 GreptimeDB {}",
            common_version::version()
        )))))
    }
}
