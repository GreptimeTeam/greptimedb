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
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

/// Converts the `JSONB` into `String`. It's useful for displaying JSONB content.
#[derive(Clone, Debug, Default)]
pub struct JsonToStringFunction;

const NAME: &str = "json_to_string";

impl Function for JsonToStringFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
        Signature::exact(vec![DataType::Binary], Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let jsons = arg0.as_binary::<i32>();

        let size = jsons.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let json = jsons.is_valid(i).then(|| jsons.value(i));
            let result = json
                .map(|json| jsonb::from_slice(json).map(|x| x.to_string()))
                .transpose()
                .map_err(|e| DataFusionError::Execution(format!("invalid json binary: {e}")))?;

            builder.append_option(result.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonToStringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_TO_STRING")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::BinaryArray;
    use datafusion_expr::TypeSignature;

    use super::*;

    #[test]
    fn test_json_to_string_function() {
        let json_to_string = JsonToStringFunction;

        assert_eq!("json_to_string", json_to_string.name());
        assert_eq!(
            DataType::Utf8View,
            json_to_string.return_type(&[DataType::Binary]).unwrap()
        );

        assert!(matches!(json_to_string.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![DataType::Binary]
        ));

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
                BinaryArray::from_iter_values(jsonbs),
            ))],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_to_string
            .invoke_with_args(args)
            .and_then(|x| x.to_array(1))
            .unwrap();
        let vector = result.as_string_view();

        assert_eq!(3, vector.len());
        for (i, gt) in json_strings.iter().enumerate() {
            let result = vector.value(i);
            // remove whitespaces
            assert_eq!(gt.replace(" ", ""), result);
        }

        let invalid_jsonb = vec![b"invalid json"];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                BinaryArray::from_iter_values(invalid_jsonb),
            ))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let vector = json_to_string.invoke_with_args(args);
        assert!(vector.is_err());
    }
}
