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

use std::fmt::{self, Display};
use std::sync::Arc;

use common_query::error::Result;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BinaryViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

/// Parses the `String` into `JSONB`.
#[derive(Clone, Debug, Default)]
pub struct ParseJsonFunction;

const NAME: &str = "parse_json";

impl Function for ParseJsonFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> Signature {
        Signature::string(1, Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let json_strings = arg0.as_string_view();

        let size = json_strings.len();
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

impl Display for ParseJsonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PARSE_JSON")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::StringViewArray;

    use super::*;

    #[test]
    fn test_get_by_path_function() {
        let parse_json = ParseJsonFunction;

        assert_eq!("parse_json", parse_json.name());
        assert_eq!(
            DataType::BinaryView,
            parse_json.return_type(&[DataType::Binary]).unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                value.to_vec()
            })
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                StringViewArray::from_iter_values(json_strings),
            ))],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = parse_json
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_binary_view();

        assert_eq!(3, vector.len());
        for (i, gt) in jsonbs.iter().enumerate() {
            let result = vector.value(i);
            assert_eq!(gt, result);
        }
    }
}
