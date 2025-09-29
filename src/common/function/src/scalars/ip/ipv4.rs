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

use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use datafusion_common::arrow::array::{Array, AsArray, StringViewBuilder, UInt32Builder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::{DataType, UInt32Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use derive_more::Display;

use crate::function::{Function, extract_args};

/// Function that converts a UInt32 number to an IPv4 address string.
///
/// Interprets the number as an IPv4 address in big endian and returns
/// a string in the format A.B.C.D (dot-separated numbers in decimal form).
///
/// For example:
/// - 167772160 (0x0A000000) returns "10.0.0.0"
/// - 3232235521 (0xC0A80001) returns "192.168.0.1"
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub struct Ipv4NumToString {
    aliases: [String; 1],
}

impl Default for Ipv4NumToString {
    fn default() -> Self {
        Self {
            aliases: ["inet_ntoa".to_string()],
        }
    }
}

impl Function for Ipv4NumToString {
    fn name(&self) -> &str {
        "ipv4_num_to_string"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        Signature::new(
            TypeSignature::Exact(vec![DataType::UInt32]),
            Volatility::Immutable,
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let uint_vec = arg0.as_primitive::<UInt32Type>();

        let size = uint_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let ip_num = uint_vec.is_valid(i).then(|| uint_vec.value(i));
            let ip_str = match ip_num {
                Some(num) => {
                    // Convert UInt32 to IPv4 string (A.B.C.D format)
                    let a = (num >> 24) & 0xFF;
                    let b = (num >> 16) & 0xFF;
                    let c = (num >> 8) & 0xFF;
                    let d = num & 0xFF;
                    Some(format!("{}.{}.{}.{}", a, b, c, d))
                }
                _ => None,
            };

            builder.append_option(ip_str.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Function that converts a string representation of an IPv4 address to a UInt32 number.
///
/// For example:
/// - "10.0.0.1" returns 167772161
/// - "192.168.0.1" returns 3232235521
/// - Invalid IPv4 format throws an exception
#[derive(Clone, Debug, Default, Display)]
#[display("{}", self.name())]
pub struct Ipv4StringToNum;

impl Function for Ipv4StringToNum {
    fn name(&self) -> &str {
        "ipv4_string_to_num"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
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
        let ip_vec = arg0.as_string_view();
        let size = ip_vec.len();
        let mut builder = UInt32Builder::with_capacity(size);

        for i in 0..size {
            let ip_str = ip_vec.is_valid(i).then(|| ip_vec.value(i));
            let ip_num = match ip_str {
                Some(ip_str) => {
                    let ip_addr = Ipv4Addr::from_str(ip_str).map_err(|_| {
                        InvalidFuncArgsSnafu {
                            err_msg: format!("Invalid IPv4 address format: {}", ip_str),
                        }
                        .build()
                    })?;
                    Some(u32::from(ip_addr))
                }
                _ => None,
            };

            builder.append_option(ip_num);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{StringViewArray, UInt32Array};

    use super::*;

    #[test]
    fn test_ipv4_num_to_string() {
        let func = Ipv4NumToString::default();

        // Test data
        let values = vec![167772161u32, 3232235521u32, 0u32, 4294967295u32];
        let input = ColumnarValue::Array(Arc::new(UInt32Array::from(values)));

        let args = ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_string_view();

        assert_eq!(result.value(0), "10.0.0.1");
        assert_eq!(result.value(1), "192.168.0.1");
        assert_eq!(result.value(2), "0.0.0.0");
        assert_eq!(result.value(3), "255.255.255.255");
    }

    #[test]
    fn test_ipv4_string_to_num() {
        let func = Ipv4StringToNum;

        // Test data
        let values = vec!["10.0.0.1", "192.168.0.1", "0.0.0.0", "255.255.255.255"];
        let input = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        let args = ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::UInt32, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_primitive::<UInt32Type>();

        assert_eq!(result.value(0), 167772161);
        assert_eq!(result.value(1), 3232235521);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 4294967295);
    }

    #[test]
    fn test_ipv4_conversions_roundtrip() {
        let to_num = Ipv4StringToNum;
        let to_string = Ipv4NumToString::default();

        // Test data for string to num to string
        let values = vec!["10.0.0.1", "192.168.0.1", "0.0.0.0", "255.255.255.255"];
        let input = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        let args = ScalarFunctionArgs {
            args: vec![input],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::UInt32, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_num.invoke_with_args(args).unwrap();

        let args = ScalarFunctionArgs {
            args: vec![result],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_string.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_string_view();

        for (i, expected) in values.iter().enumerate() {
            assert_eq!(result.value(i), *expected);
        }
    }
}
