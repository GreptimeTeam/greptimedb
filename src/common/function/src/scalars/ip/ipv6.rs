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

use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use common_query::error::InvalidFuncArgsSnafu;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BinaryViewBuilder, StringViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use derive_more::Display;

use crate::function::{Function, extract_args};

/// Function that converts a hex string representation of an IPv6 address to a formatted string.
///
/// For example:
/// - "20010DB8000000000000000000000001" returns "2001:db8::1"
/// - "00000000000000000000FFFFC0A80001" returns "::ffff:192.168.0.1"
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct Ipv6NumToString {
    signature: Signature,
}

impl Default for Ipv6NumToString {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl Function for Ipv6NumToString {
    fn name(&self) -> &str {
        "ipv6_num_to_string"
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
        let [arg0] = extract_args(self.name(), &args)?;

        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let hex_vec = arg0.as_string_view();
        let size = hex_vec.len();
        let mut builder = StringViewBuilder::with_capacity(size);

        for i in 0..size {
            let hex_str = hex_vec.is_valid(i).then(|| hex_vec.value(i));
            let ip_str = match hex_str {
                Some(s) => {
                    let hex_str = s.to_lowercase();

                    // Validate and convert hex string to bytes
                    let bytes = if hex_str.len() == 32 {
                        let mut bytes = [0u8; 16];
                        for i in 0..16 {
                            let byte_str = &hex_str[i * 2..i * 2 + 2];
                            bytes[i] = u8::from_str_radix(byte_str, 16).map_err(|_| {
                                InvalidFuncArgsSnafu {
                                    err_msg: format!("Invalid hex characters in '{}'", byte_str),
                                }
                                .build()
                            })?;
                        }
                        bytes
                    } else {
                        return Err(DataFusionError::Execution(format!(
                            "expecting 32 hex characters, got {}",
                            hex_str.len()
                        )));
                    };

                    // Convert bytes to IPv6 address
                    let addr = Ipv6Addr::from(bytes);

                    // Special handling for IPv6-mapped IPv4 addresses
                    if let Some(ipv4) = addr.to_ipv4() {
                        if addr.octets()[0..10].iter().all(|&b| b == 0)
                            && addr.octets()[10] == 0xFF
                            && addr.octets()[11] == 0xFF
                        {
                            Some(format!("::ffff:{}", ipv4))
                        } else {
                            Some(addr.to_string())
                        }
                    } else {
                        Some(addr.to_string())
                    }
                }
                _ => None,
            };

            builder.append_option(ip_str.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Function that converts a string representation of an IPv6 address to its binary representation.
///
/// For example:
/// - "2001:db8::1" returns its binary representation
/// - If the input string contains a valid IPv4 address, returns its IPv6 equivalent
/// - HEX can be uppercase or lowercase
/// - Invalid IPv6 format throws an exception
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct Ipv6StringToNum {
    signature: Signature,
}

impl Default for Ipv6StringToNum {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl Function for Ipv6StringToNum {
    fn name(&self) -> &str {
        "ipv6_string_to_num"
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::BinaryView)
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
        let ip_vec = arg0.as_string_view();

        let size = ip_vec.len();
        let mut builder = BinaryViewBuilder::with_capacity(size);

        for i in 0..size {
            let ip_str = ip_vec.is_valid(i).then(|| ip_vec.value(i));
            let ip_binary = match ip_str {
                Some(addr_str) => {
                    let addr = if let Ok(ipv6) = Ipv6Addr::from_str(addr_str) {
                        // Direct IPv6 address
                        ipv6
                    } else if let Ok(ipv4) = Ipv4Addr::from_str(addr_str) {
                        // IPv4 address to be converted to IPv6
                        ipv4.to_ipv6_mapped()
                    } else {
                        // Invalid format
                        return Err(DataFusionError::Execution(format!(
                            "Invalid IPv6 address format: {}",
                            addr_str
                        )));
                    };

                    // Convert IPv6 address to binary (16 bytes)
                    let octets = addr.octets();
                    Some(octets.to_vec())
                }
                _ => None,
            };

            builder.append_option(ip_binary.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::StringViewArray;

    use super::*;

    #[test]
    fn test_ipv6_num_to_string() {
        let func = Ipv6NumToString::default();

        // Hex string for "2001:db8::1"
        let hex_str1 = "20010db8000000000000000000000001";

        // Hex string for IPv4-mapped IPv6 address "::ffff:192.168.0.1"
        let hex_str2 = "00000000000000000000ffffc0a80001";

        let values = vec![hex_str1, hex_str2];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(2).unwrap();
        let result = result.as_string_view();

        assert_eq!(result.value(0), "2001:db8::1");
        assert_eq!(result.value(1), "::ffff:192.168.0.1");
    }

    #[test]
    fn test_ipv6_num_to_string_uppercase() {
        let func = Ipv6NumToString::default();

        // Uppercase hex string for "2001:db8::1"
        let hex_str = "20010DB8000000000000000000000001";

        let values = vec![hex_str];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(1).unwrap();
        let result = result.as_string_view();

        assert_eq!(result.value(0), "2001:db8::1");
    }

    #[test]
    fn test_ipv6_num_to_string_error() {
        let func = Ipv6NumToString::default();

        // Invalid hex string - wrong length
        let hex_str = "20010db8";

        let values = vec![hex_str];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        // Should return an error
        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args);
        assert!(result.is_err());

        // Check that the error message contains expected text
        let error_msg = result.unwrap_err().to_string();
        assert_eq!(
            error_msg,
            "Execution error: expecting 32 hex characters, got 8"
        );
    }

    #[test]
    fn test_ipv6_string_to_num() {
        let func = Ipv6StringToNum::default();

        let values = vec!["2001:db8::1", "::ffff:192.168.0.1", "192.168.0.1"];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = func.invoke_with_args(args).unwrap();
        let result = result.to_array(3).unwrap();
        let result = result.as_binary_view();

        // Expected binary for "2001:db8::1"
        let expected_1 = [
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
        ];

        // Expected binary for "::ffff:192.168.0.1" or "192.168.0.1" (IPv4-mapped)
        let expected_2 = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 0, 0x01,
        ];

        assert_eq!(result.value(0), &expected_1);
        assert_eq!(result.value(1), &expected_2);
        assert_eq!(result.value(2), &expected_2);
    }

    #[test]
    fn test_ipv6_conversions_roundtrip() {
        let to_num = Ipv6StringToNum::default();
        let to_string = Ipv6NumToString::default();

        // Test data
        let values = vec!["2001:db8::1", "::ffff:192.168.0.1"];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&values)));

        // Convert IPv6 addresses to binary
        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_num.invoke_with_args(args).unwrap();

        // Convert binary to hex string representation (for ipv6_num_to_string)
        let mut hex_strings = Vec::new();
        let result = result.to_array(2).unwrap();
        let binary_vector = result.as_binary_view();

        for i in 0..binary_vector.len() {
            let bytes = binary_vector.value(i);
            let hex = bytes.iter().fold(String::new(), |mut acc, b| {
                write!(&mut acc, "{:02x}", b).unwrap();
                acc
            });
            hex_strings.push(hex);
        }

        let hex_str_refs: Vec<&str> = hex_strings.iter().map(|s| s.as_str()).collect();
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&hex_str_refs)));

        // Now convert hex to formatted string
        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_string.invoke_with_args(args).unwrap();
        let result = result.to_array(2).unwrap();
        let result = result.as_string_view();

        // Compare with original input
        assert_eq!(result.value(0), values[0]);
        assert_eq!(result.value(1), values[1]);
    }

    #[test]
    fn test_ipv6_conversions_hex_roundtrip() {
        // Create a new test to verify that the string output from ipv6_num_to_string
        // can be converted back using ipv6_string_to_num
        let to_string = Ipv6NumToString::default();
        let to_binary = Ipv6StringToNum::default();

        // Hex representation of IPv6 addresses
        let hex_values = vec![
            "20010db8000000000000000000000001",
            "00000000000000000000ffffc0a80001",
        ];
        let arg0 = ColumnarValue::Array(Arc::new(StringViewArray::from_iter_values(&hex_values)));

        // Convert hex to string representation
        let args = ScalarFunctionArgs {
            args: vec![arg0],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_string.invoke_with_args(args).unwrap();

        // Then convert string representation back to binary
        let args = ScalarFunctionArgs {
            args: vec![result],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = to_binary.invoke_with_args(args).unwrap();
        let result = result.to_array(2).unwrap();
        let result = result.as_binary_view();

        // Expected binary values
        let expected_bin1 = [
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
        ];
        let expected_bin2 = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xC0, 0xA8, 0, 0x01,
        ];

        assert_eq!(result.value(0), &expected_bin1);
        assert_eq!(result.value(1), &expected_bin2);
    }
}
