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

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BinaryViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use derive_more::derive::Display;

use crate::function::{Function, extract_args};

/// Parses the `String` into `JSONB`.
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct ToJsonFunction {
    signature: Signature,
}

impl Default for ToJsonFunction {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

const NAME: &str = "to_json";

impl Function for ToJsonFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Null)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let json_strings = arg0.as_string_view();

        let size = json_strings.len();

        // TODO(sunng87): modify this
        let mut builder = BinaryViewBuilder::with_capacity(size);

        for i in 0..size {
            let s = json_strings.is_valid(i).then(|| json_strings.value(i));
            let result = s
                .map(|s| {
                    jsonb::parse_value(s.as_bytes())
                        .map(|x| x.to_vec())
                        .map_err(|e| DataFusionError::Execution(format!("cannot parse '{s}': {e}")))
                })
                .transpose()?;
            builder.append_option(result.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
